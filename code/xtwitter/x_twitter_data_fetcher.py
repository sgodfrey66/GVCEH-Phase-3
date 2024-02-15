
import os
import pickle
import time

from datetime import datetime, timedelta

# Logging and monitoring
import logging

import tweepy as tw
import pandas as pd

class GVCEHXTwitter():
    '''
    Class to handle Twitter API calls for the GVCEH project.

    Primary output are the tweet files found in the tweets file path. Note
    that the logic within the fetch_data excludes any submission IDs already
    in the tweets output file.

    Inputs:
        __init__ :
            bearer_token: X_Twitter bearer token
            consumer_key: X_Twitter consumer key
            consumer_secret: X_Twitter consumer secret
            access_token: X_Twitter access token
            access_token_secret: X_Twitter access token secret
            fetch_logging: True if logging should be one

    Outputs:
        batch_scrape:
            Posts files found in the posts_file_path
            Log files found in the logs_file_path

    Attributes:
        query_file_path: Path to the queries file
        tweets_file_path: Path to the retrieved posts or submissions
        logs_file_path: Path to the logs captured during retrieval
        keywords_file_path: Path to the CSV files with keyword search terms

        max_tweets: Maximum returned tweets
        query_start_at: Index of first query from query cache to start the batch_scrape

        tweet_fields: Tweet fields to return from API calls
        user_fields: User fields to return from API calls
        place_fields: Place fields to return from API calls
        expansions: Expanded fields to return from API calls (beyond standard API response)

        fetch_logging: Boolean to turn logging on and off

    '''

    # Data files paths
    query_file_path = "../../data/xtwitter/queries"
    tweets_file_path = "../../data/xtwitter/tweets"
    logs_file_path = "../../data/xtwitter/logs"
    keywords_file_path = "../../data/keywords"

    # Logging flag
    fetch_logging = True

    ### setting up the config
    max_tweets = 100  # if we want to return less than the API's max
    query_start_at = 0

    # API return fields
    tweet_fields = ["context_annotations", "public_metrics", "created_at",
                    "text", "source", "geo",]
    user_fields = ["name", "username", "location", "verified", "description",
                   "public_metrics",]
    place_fields = ["country", "geo", "name", "place_type"]
    expansions = ["author_id", "geo.place_id", "referenced_tweets.id"]

    # Sleep between API calls (seconds)
    api_sleep_time = 2.5


    def __init__(self,
                 bearer_token,
                 consumer_key,
                 consumer_secret,
                 access_token,
                 access_token_secret,
                 fetch_logging=True,
                 **kwargs
                 ):
        '''
        Initialize the GVCEXTwitter class.
        '''

        # Update any key word args
        self.__dict__.update(kwargs)
        
        # establish a tweepy client
        self.client = tw.Client(bearer_token=bearer_token,
                                consumer_key=consumer_key,
                                consumer_secret=consumer_secret,
                                access_token=access_token,
                                access_token_secret=access_token_secret)
        
        # Turn off logging if needed
        if fetch_logging != True:
            self.fetch_logging = False
            

    def query_twitter(self,
                      search_query, 
                      relevant_region, 
                      start_time, 
                      end_time, 
                      seven_days=False):
        """
        Method to run one query against the API and store it
        """

        return_data = []
        search_query = search_query.replace(" and ", ' "and" ')

        if seven_days:
            start_time = None
            end_time = None

        # get tweets
        ### limits us last 7 days, need elevated account for longer than that
        tweets = self.client.search_recent_tweets(query=search_query,
                                                  start_time=start_time,
                                                  end_time=end_time,
                                                  tweet_fields=self.tweet_fields,
                                                  user_fields=self.user_fields,
                                                  max_results=self.max_tweets,
                                                  place_fields=self.place_fields,
                                                  expansions=self.expansions,)

        ### not yielding anything? exit early
        if not tweets.data:
            return []

        ### generate our place information
        if "places" in tweets.includes.keys():
            place_info = {
                place.id: {
                    "bbox": place.geo[
                        "bbox"
                    ],  # geoJSON, min long, min lat, max long, max lat
                    "full_name": place.full_name,
                    # place.name
                    # place.place_type
                    # place.full_name
                    # place.country
                }
                for place in tweets.includes["places"]
            }

            ### generate our xtwitter xtwitter
        for tweet, user in zip(tweets.data, tweets.includes["users"]):

            newtweet = {}

            # original text
            newtweet["text"] = tweet.text

            ### working on quote tweets:
            if tweet.referenced_tweets:
                # print(tweet.text)
                for thist in tweet.referenced_tweets:
                    if thist.data["type"] == "quoted":
                        qt = client.get_tweet(thist.data["id"], tweet_fields=["text"])

                        mergetweet = (
                                newtweet["text"].strip() + " " + qt.data["text"].strip()
                        )
                        mergetweet = mergetweet.replace("\n", "")

                        newtweet["text"] = mergetweet

            ### scrape time
            newtweet["scrape_time"] = str(datetime.now())

            ### unique ID
            newtweet["tweet_id"] = tweet.id

            # post time
            newtweet["created_at"] = str(tweet.created_at)

            # reply count
            newtweet["reply_count"] = tweet.public_metrics["reply_count"]
            # number of quote tweets
            newtweet["quote_count"] = tweet.public_metrics["quote_count"]
            # number of likes
            newtweet["like_count"] = tweet.public_metrics["like_count"]
            # number of RTs
            newtweet["retweet_count"] = tweet.public_metrics["retweet_count"]

            ### geo xtwitter (where available)
            newtweet["geo_full_name"] = None
            newtweet["geo_id"] = None
            newtweet["geo_bbox"] = None

            if tweet.geo:
                newtweet["geo_id"] = tweet.geo["place_id"]
                newtweet["geo_full_name"] = place_info[tweet.geo["place_id"]]["full_name"]
                newtweet["geo_bbox"] = place_info[tweet.geo["place_id"]]["bbox"]

            ### cordinate xtwitter - where available
            newtweet["tweet_coordinate"] = ""
            if tweet.geo:
                if tweet.geo.get("coordinates", None):
                    newtweet["tweet_coordinate"] = tweet.geo.get("coordinates").get(
                        "coordinates"
                    )

            # poster
            newtweet["username"] = user.username

            ### user profile location
            newtweet["user_location"] = user.location

            # number of followers
            newtweet["num_followers"] = user.public_metrics["followers_count"]

            ### so we know how it was found
            newtweet["search_keywords"] = search_query

            ### more meta xtwitter
            newtweet["search_neighbourhood"] = relevant_region

            return_data.append(newtweet)

        return return_data


    def batch_scrape(self,
                     seven_days=False):
        '''
        Method to run a batch of API queries calling the query_twitter method
        each time.

        Queries are loaded from the query_cache_file found at the query_file_path.

        '''


        # Establish date format
        dtformat = "%Y-%m-%dT%H:%M:%SZ"

        # Start logger
        self.log_event(msg_id=0, screen_print=False, logfile_stub='xtwitter')

        # Log fetch start
        self.log_event(msg_id=1, screen_print=True, event='start fetch', source='xtwitter')

        ### open our pickle cache of queries
        # https://stackoverflow.com/questions/25464295/dump-a-list-in-a-pickle-file-and-retrieve-it-back-later
        try:

            # Open query file
            query_cache_file = "querylist.pkl"
            with open(os.path.join(self.query_file_path, query_cache_file), "rb") as f:
                query_cache = pickle.load(f)

            our_queries = query_cache[self.query_start_at:]

            # Log file found
            self.log_event(msg_id=1, screen_print=False, event='query file loaded',
                           query_cnt=len(our_queries))

        except:

            # Query file not found
            # Log file found
            self.log_event(msg_id=1, screen_print=False, event='query file not found',
                           query_cnt=len(our_queries))

            raise RuntimeError("Query file not found: {}".format(os.path.join(self.query_file_path, query_cache_file)))

        # Read the stored tweet history
        try:
            # Set history file name
            history_file = "xtwitter_tweets.csv"

            # History file found
            history_df = pd.read_csv(os.path.join(self.tweets_file_path, history_file))
            history_cnt = len(history_df)

            # Get list  of tweet ids
            history_tweet_ids = history_df['tweet_id'].unique().tolist()

            # Log file found
            self.log_event(msg_id=1, screen_print=False, event='history file loaded',
                           history_row_cnt=history_cnt)

        except:
            # No history file found
            history_cnt = 0
            history_df = None
            history_tweet_ids = []

            # Log file not found
            self.log_event(msg_id=1, screen_print=False, event='No history file found',
                           history_row_cnt=history_cnt)


        # Start counts
        num_queries = 0
        num_results = 0

        ### Get times
        current_time = datetime.utcnow()
        start_time = current_time - timedelta(days=2)

        # Subtracting 15 seconds because api needs end_time must be a minimum of 10
        # seconds prior to the request time
        end_time = current_time - timedelta(seconds=15)

        # convert to strings
        start_time, end_time = start_time.strftime(dtformat), end_time.strftime(dtformat)

        data_found = False
        for q in our_queries:

            num_queries += 1

            try:

                # Get data from API
                this_query = q[0].replace("&", "")
                data = self.query_twitter(search_query=this_query, 
                                          relevant_region=q[1], 
                                          start_time=start_time, 
                                          end_time=end_time, 
                                          seven_days=seven_days)

                num_results += len(data)

                ### scave our xtwitter - only if we got any
                if data:
                    data_cleaned = [
                        {
                            k: v
                            for k, v in d.items()
                            if k not in ("geo_bbox", "tweet_coordinate")
                        }
                        for d in data
                    ]
                    df = pd.DataFrame(data_cleaned)

                    # Create mask to remove tweets already in history
                    mask = ~df['tweet_id'].isin(history_tweet_ids)
                    # Only include tweets not already seen
                    df = df[mask]
                    df = df.reset_index()

                    # If this is first time data has been returned from API
                    if not data_found:
                        final_results = df
                        data_found = True

                    # If not add to previous data
                    final_results = pd.concat(objs=[final_results, df])

                    # Log success
                    self.log_event(msg_id=1, screen_print=False, event='fetch success',
                                   query_num=num_queries,
                                   sample_response=final_results.tail(1), query=q[0])

                else:

                    # Log no data returned
                    self.log_event(msg_id=1, screen_print=False, event='fetch no data',
                                   query=q[0])

            except Exception as e:

                # Log exception
                self.log_event(msg_id=1, screen_print=False, event='fetch exception',
                               exception_info=str(e), query_num=num_queries,
                               num_results=num_results, query=q[0])

                break

            time.sleep(self.api_sleep_time)

        # Write new file
        if type(history_df) != type(None):
            new_data_df = pd.concat(objs=[history_df, final_results])
        else:
            new_data_df = final_results

        # Drop duplicates
        new_data_df = new_data_df.drop_duplicates(subset=['tweet_id'])

        # Save the file
        new_data_df.to_csv(path_or_buf=os.path.join(self.tweets_file_path, history_file),
                           index=False)

        # Log file update - finished fetch
        self.log_event(msg_id=1, screen_print=True, event='fetch complete')

        # Close logging
        self.log_event(msg_id=-1, screen_print=False)

    def log_event(self,
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

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        # msg_id = -1 - Close logger
        if self.fetch_logging and msg_id == -1:

            # Close any open logs
            logging.shutdown()

        # msg_id = 0 - Set up the logger
        if self.fetch_logging and msg_id == 0:

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
        elif self.fetch_logging and msg_id == 1:

            # Create a message using terms in the kwargs
            log_msg = ": ".join(["{}: {}".format(k, kwargs[k]) for k in kwargs.keys()])

            # Add the time
            log_msg = "time: {}: {}".format(current_time, log_msg)
            if screen_print:
                print(log_msg)
            self.logger.info(msg=log_msg)

