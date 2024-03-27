# Functions to score X tweets for relevance and sentiment
import os, sys
import pandas as pd

from datetime import datetime

# Logging and monitoring
import logging

from transformers import pipeline
from setfit import SetFitModel


class ScoreTweets():
    '''
    Class to score tweets for relevance and sentiment.  These relevance and sentiment models
    were created in Phase 2 of the SWB-GVCEH project.  However, this class and its use in this
    pipeline was added during Phase 3

    Attributes:
        tweets_file_path: Path to the retrieved posts or submissions
        logs_file_path: Path to the logs captured during retrieval
        new_input_file_name: Filename for new tweets that need to be scored
        scored_input_file_name: Filename for tweets that have been previously scored

        relevance_model_hf_location: Hugging Face location for relevance model
        sentiment_model_hf_location: Hugging Face location for sentiment model

        df: pandas dataframe containing a column with tweet text ("text")
        update_scores: Boolean indicating if scores should be updated and overwritten (True)
                        or only new scores should be added for not scored tweets

        fetch_logging: Boolean to turn logging on and off
        dtformat: String format for time values



    '''

    # Data files paths
    tweets_file_path = "../data/xtwitter/tweets"
    logs_file_path = "../data/xtwitter/logs"

    # Input files
    new_input_file_name = "xtwitter_tweets.csv"
    scored_input_file_name = "xtwitter_tweets_scored.csv"

    # Model parameters
    relevance_model_hf_location = "sheilaflood/gvceh-setfit-rel-model2"
    sentiment_model_hf_location = "cardiffnlp/twitter-roberta-base-sentiment-latest"

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

        # Update any key word args
        self.__dict__.update(kwargs)

        # Set update flag
        self.update_scores = update_scores

        # Turn off logging if needed
        if score_logging != True:
            self.score_logging = False

        # Start logger
        self.__log_event(msg_id=0, screen_print=False, logfile_stub='xtwitter_scorer')

        # Log score start
        self.__log_event(msg_id=1, screen_print=True, event='start score', source='xtwitter')

        # Read tweet data
        self.read_tweet_file()

        # Score for relevance
        self.score_relevance()

        # Score for sentiment
        self.sentiment_model()

        # Score for relevance
        self.write_tweet_file()

        # Close logging
        self.__log_event(msg_id=-1, screen_print=False)

    def score_relevance(self):
        '''
        Function to score tweets for sentiment.  This model was created during
        Phase 2 of the SWB-GVCEH project.

        '''

        # Log relevance score start
        self.__log_event(msg_id=1, screen_print=True, event='start relevance scoring', source='xtwitter')

        # create the model
        model = SetFitModel.from_pretrained(self.relevance_model_hf_location)

        # Put the text columns into a list
        all_text = self.df_new['text'].tolist()

        # Score the text
        all_results = model(all_text)

        # Convert to values
        all_results = all_results.cpu().numpy()

        # add a is_relevant column
        self.df_new['is_relevant'] = all_results

        # Log relevance score completion
        self.__log_event(msg_id=1, screen_print=True, event='relevance scoring completed', source='xtwitter')


    def sentiment_model(self):
        '''
        Function to score tweets for sentiment.  Model was created during
        Phase 2 of the SWB-GVCEH project.

        :param df:
        :return:
        '''

        # Log sentiment score start
        self.__log_event(msg_id=1, screen_print=True, event='start sentiment scoring', source='xtwitter')

        # create the model ( #-1 = cpu, 0 = gpu )
        model = pipeline(task="sentiment-analysis",
                         model=self.sentiment_model_hf_location,
                         device=-1)
        model.tokenizer.model_max_length = 512


        # Put the text columns into a list
        all_text = self.df_new['text'].tolist()

        all_res = []

        for res in model(all_text, batch_size=32, truncation=True):
            all_res.append(res)

        all_sentiments = [x['label'] for x in all_res]
        all_scores = [x['score'] for x in all_res]

        self.df_new['sentiment'] = all_sentiments
        self.df_new['sentiment_score'] = all_scores

        # Log sentiment score start
        self.__log_event(msg_id=1, screen_print=True, event='sentiment scoring completed', source='xtwitter')


    def read_tweet_file(self):
        '''
        Method to read tweets data from a file and store in a pandas datafrae

        '''

        # Update that now reading files
        self.__log_event(msg_id=1, screen_print=True, event='read new and scored tweets', source='xtwitter')

        # Get new tweets
        try:
            self.df_new = pd.read_csv(filepath_or_buffer=os.path.join(self.tweets_file_path,
                                                                      self.new_input_file_name))

        except:

            # Log submission processing
            msg = ("New tweets file not found: name: {}").format(os.path.join(self.tweets_file_path,
                                                                              self.new_input_file_name))
            self.__log_event(msg_id=1, screen_print=True, event='processing error', error_msg=msg)

            raise RuntimeError(msg)

        # Look for an existing file with scored tweet data
        try:
            self.df_scored = pd.read_csv(filepath_or_buffer=os.path.join(self.tweets_file_path,
                                                                         self.scored_input_file_name))

            self.scored_file_found = True

        except:

            self.scored_file_found = False

        # If we're updating everything create one big data frame
        # Otherwise just score the new tweets and then concat with previously scored
        # before writing (assuming scored file found)
        if self.update_scores and self.scored_file_found:

            self.df_new = pd.concat(objs=[self.df_new, self.df_scored])


    def write_tweet_file(self):
        '''
        Method to save tweets data to a file after scoring

        '''

        # Update that now writing files
        self.__log_event(msg_id=1, screen_print=True, event='write scored tweets', source='xtwitter')

        # Combine new and scored dataframes before writing if only updating
        if not self.update_scores and self.scored_file_found:

            self.df_new = pd.concat(objs=[self.df_new, self.df_scored])

        # Save everything
        self.df_new.to_csv(path_or_buf=os.path.join(self.tweets_file_path,
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