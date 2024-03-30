# Class to score Reddit posts for relevance and sentiment
import os, sys
import json

import pandas as pd

from datetime import datetime

# Logging and monitoring
import logging

from transformers import pipeline
import joblib
# from setfit import SetFitModel

from tempfile import TemporaryFile

# GCP
from google.cloud import storage
from google.oauth2 import service_account


# GVCEH objectscl
sys.path.insert(0, "utils/")
import gcp_tools as gt



class ScorePosts():
    '''
    Class to score posts for relevance and sentiment.  These relevance and sentiment models
    were created in Phase 3 of the SWB-GVCEH project.  In additioin, this class and its use in this
    pipeline was also added during Phase 3

    Attributes:
        posts_file_path: Path to the retrieved posts or submissions
        logs_file_path: Path to the logs captured during retrieval
        new_input_file_name: Filename for new posts that need to be scored
        scored_input_file_name: Filename for posts that have been previously scored

        relevance_model_path: Path to the location for a locally stored relevance model
        relevance_model1_filename: Filename for the posts relevance model
        sentiment_model_hf_location: Hugging Face location for sentiment model

        ggcp_credentials: GCP project credentials used to interface with GCP storage

        df: pandas dataframe containing a column with tweet text ("text")
        update_scores: Boolean indicating if scores should be updated and overwritten (True)
                        or only new scores should be added for not scored posts

        fetch_logging: Boolean to turn logging on and off
        dtformat: String format for time values

    '''

    # Data files paths
    posts_file_path = "../data/reddit/posts"
    logs_file_path = "../data/reddit/logs"

    # Input files
    new_input_file_name = "reddit_posts.csv"
    scored_input_file_name = "reddit_posts_scored.csv"

    # Model parameters
    relevance_model_path = "../data/models"
    relevance_model1_filename = "reddit-setfit-model.joblib"
    sentiment_model_hf_location = "cardiffnlp/twitter-roberta-base-sentiment-latest"

    # GCP Credentials
    gcp_credentials = ""

    # Flag indicating if old scores should be overwritten
    update_scores = False

    # Logging flag
    score_logging = True

    # Date format
    dtformat = "%Y-%m-%d %H:%M:%S"

    def __init__(self,
                 update_scores=False,
                 score_logging=True,
                 **kwargs
                 ):
        '''
        Initialize the ScoreTweets class.
        '''

        print('Okay we are here')

        # Update any key word args
        self.__dict__.update(kwargs)

        # Set update flag
        self.update_scores = update_scores

        # Turn off logging if needed
        if score_logging != True:
            self.score_logging = False

        # Start logger
        self.__log_event(msg_id=0, screen_print=False, logfile_stub='reddit_scorer')

        # Log score start
        self.__log_event(msg_id=1, screen_print=True, event='start score', source='reddit')

        # Read tweet data
        self.read_posts_file()

        # Score for relevance
        self.score_relevance()

        # Score for sentiment
        self.sentiment_model()

        # Score for relevance
        self.write_posts_file()

        # Close logging
        self.__log_event(msg_id=-1, screen_print=False)

    def score_relevance(self):
        '''
        Method to score posts for sentiment.

        '''

        # Log relevance score start
        self.__log_event(msg_id=1, screen_print=True, event='start relevance scoring', source='reddit')

        # create the model
        prefix = "gs://"
        # If on GCP need some code to open the file
        if self.relevance_model_path.find(prefix) >= 0:

            # Get bucket name and model_bucket from model path
            prefix_len = len(prefix)
            bucket_name = self.relevance_model_path[prefix_len: self.relevance_model_path[5:].find("/") + prefix_len]
            model_bucket = self.relevance_model_path[prefix_len:]
            model_bucket = "{}/{}".format(model_bucket[model_bucket.find("/") + 1:],
                                          self.relevance_model1_filename)

            # # Set up GCP storage client
            json_acct_info = json.loads(self.gcp_credentials)
            credentials = service_account.Credentials.from_service_account_info(json_acct_info)
            storage_client = storage.Client(credentials=credentials, project=json_acct_info["project_id"])

            # Create GCP buckets
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(model_bucket)

            # Download model to temporary file
            with TemporaryFile() as temp_file:
                # download blob into temp file
                blob.download_to_file(temp_file)
                temp_file.seek(0)
                # load into joblib
                model1 = joblib.load(temp_file)

        # Otherwise we can load locally
        else:
            model1 = joblib.load(os.path.join(self.relevance_model_path,
                                              self.relevance_model1_filename))

        # Replace NA values with blanks
        for col in ['selftext', 'title']:
            self.df_new[col] = self.df_new[col].fillna(value=' ')

        # Combined title and text into a single column
        self.df_new['titletext'] = self.df_new['title'] + " " + self.df_new['selftext']

        # Put the text columns into a list
        all_text = self.df_new['titletext'].tolist()

        # List to hold predictions
        predictions = []

        for post in all_text:
            # Predict for each title individually
            prediction = model1.model.predict([post])

            # Assuming prediction is a list with a single element
            predictions.append(prediction[0].tolist())

        # add a is_relevant column
        self.df_new['is_relevant'] = predictions

        # Log relevance score completion
        self.__log_event(msg_id=1, screen_print=True, event='relevance scoring completed', source='reddit')


    def sentiment_model(self):
        '''
        Method to score posts for sentiment.

        '''

        # Log sentiment score start
        self.__log_event(msg_id=1, screen_print=True, event='start sentiment scoring', source='reddit')

        # Initialize sentiment analysis pipeline
        sentiment_analyzer = pipeline(task='sentiment-analysis',
                                      model=self.sentiment_model_hf_location,
                                      device=-1,
                                      truncation=True)
        sentiment_analyzer.tokenizer.model_max_length = 512

        # Initialize lists to store results
        sentiments = []
        scores = []

        # Process each post in the dataframe
        for post in self.df_new['titletext']:
            # Segment the text
            segments = post.split('\n')

            # Initialize counters for sentiments and scores
            sentiment_counts = {'positive': 0, 'negative': 0, 'neutral': 0}
            sentiment_scores = {'positive': 0, 'negative': 0, 'neutral': 0}

            # Analyze sentiment for each segment
            for segment in segments:
                # Optionally, filter out segments that are too short or not meaningful
                if len(segment.strip()) > 0:
                    result = sentiment_analyzer(segment)[0]
                    sentiment_label = result['label']
                    sentiment_score = result['score']

                    # Update sentiment and score counters
                    sentiment_counts[sentiment_label] += 1
                    sentiment_scores[sentiment_label] += sentiment_score

            # Determine overall sentiment based on total scores
            if sum(sentiment_counts.values()) > 0:  # Check if any segment with non-zero sentiment scores
                overall_sentiment = max(sentiment_scores, key=sentiment_scores.get)
                overall_sentiment_score = sentiment_scores[overall_sentiment] / sentiment_counts[overall_sentiment]
            else:
                overall_sentiment = None
                overall_sentiment_score = None

            # Append results to lists
            sentiments.append(overall_sentiment)
            scores.append(overall_sentiment_score)

        # Add sentiment and score columns to the new dataframe
        self.df_new['sentiment'] = sentiments
        self.df_new['sentiment_score'] = scores

        # Log sentiment score start
        self.__log_event(msg_id=1, screen_print=True, event='sentiment scoring completed', source='reddit')


    def read_posts_file(self):
        '''
        Method to read posts data from a file and store in a pandas datafrae

        '''

        # Update that now reading files
        self.__log_event(msg_id=1, screen_print=True, event='read new and scored posts', source='reddit')

        # Get new posts
        try:
            self.df_new = pd.read_csv(filepath_or_buffer=os.path.join(self.posts_file_path,
                                                                      self.new_input_file_name))

        except:

            # Log submission processing
            msg = ("New posts file not found: name: {}").format(os.path.join(self.posts_file_path,
                                                                             self.new_input_file_name))
            self.__log_event(msg_id=1, screen_print=True, event='processing error', error_msg=msg)

            raise RuntimeError(msg)

        # Look for an existing file with scored tweet data
        try:
            self.df_scored = pd.read_csv(filepath_or_buffer=os.path.join(self.posts_file_path,
                                                                         self.scored_input_file_name))

            self.scored_file_found = True

        except:

            self.scored_file_found = False

        # If we're updating everything create one big data frame
        # Otherwise just score the new posts and then concat with previously scored
        # before writing (assuming scored file found)
        if self.update_scores and self.scored_file_found:

            self.df_new = pd.concat(objs=[self.df_new, self.df_scored])


    def write_posts_file(self):
        '''
        Method to save posts data to a file after scoring

        '''

        # Update that now writing files
        self.__log_event(msg_id=1, screen_print=True, event='write scored posts', source='reddit')

        # Combine new and scored dataframes before writing if only updating
        if not self.update_scores and self.scored_file_found:

            self.df_new = pd.concat(objs=[self.df_new, self.df_scored])

        # Save everything
        self.df_new.to_csv(path_or_buf=os.path.join(self.posts_file_path,
                                                    self.scored_input_file_name),
                           index=False)

    def __log_event(self,
                  msg_id: int,
                  screen_print: bool,
                  **kwargs):
        '''
        Method to record a log event

        Input:
            msg_id:
                number indicating which type of message to log [only -1, 0, 1 valid]
                Notes:
                    msg_id = 0 - initialize a logger
                    msg_id = -1 - close logging
                    msg_id = 1 - construct and log entry
            screen_print: bool
                True if the log message should also be printed to the screen
            **kwargs: dict
                A dictionary of terms to include in the message
                Note that if msg_id = 0 - kwargs is expected to have a
                logfile_stub entry - kwargs['logfile_stub'] = 'victoria'

        '''

        current_time = datetime.now().strftime(self.dtformat)


        # msg_id = -1 - Close logger
        if self.score_logging and msg_id == -1:

            # Close any open logs
            logging.shutdown()

        # msg_id = 0 - Set up the logger
        if self.score_logging and msg_id == 0:

            # Close any open logs
            logging.shutdown()

            # Name a new log file
            log_file = f"{kwargs['logfile_stub']}_logfile.log"

            # Configure the logging system
            logging.basicConfig(filename=os.path.join(self.logs_file_path, log_file),
                                filemode='w',
                                format='%(levelname)s - %(message)s',
                                level=logging.INFO,
                                force=True)

            # Create a logger
            self.logger = logging.getLogger()

        # msg_id = 1 - Data fetch start
        elif self.score_logging and msg_id == 1:

            # Create a message using terms in the kwargs
            log_msg = ": ".join(["{}: {}".format(k, kwargs[k]) for k in kwargs.keys()])

            # Add the time
            log_msg = "time: {}: {}".format(current_time, log_msg)
            if screen_print:
                print(log_msg)
            self.logger.info(msg=log_msg)