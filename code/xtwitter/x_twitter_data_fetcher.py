
import os
import time
from collections import deque

from datetime import datetime, timedelta

# Logging and monitoring
import logging

import tweepy as tw
import pandas as pd

class GVCEHXTwitter():
    '''
    Class to handle Twitter API calls for the GVCEH project.

    Primary output is the tweet file found in the tweets file path. New data are appended to this file
    every time the batch scrape is run. Note that the logic within the fetch_data excludes any submission IDs
    already existing in the tweets output file.

    As of Mar 2024, access to X's full-archive search using the API is limited to Pro and Enterprise plans which
    start at $5,000/month. Since it's unlikely that this accounts using this wrapper have that level of access
    it's assumed that any searches over the past 7 days which is available to lower-tier access.  However, historical
    searches can be accessed by setting the search_start_time and search_end_time variables to dates (object type
    datetime) overriding their default values of None.  The object attribute seven-days is a flag indicating if the
    search is over 7 days or not.

    References:
        Tweepy: https://www.tweepy.org/
        Twitter API Access levels: https://developer.twitter.com/en/docs/twitter-api/getting-started/about-twitter-api
        API Rate Limits: https://developer.twitter.com/en/docs/twitter-api/rate-limits
        Search Tweets: https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#limits

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

        hashtags_file_name: File containing hashtag or other key search terms
        keywords_file_name: File containing keywords

        search_qualifiers: Addition search qualifiers beyond search terms and keywords

        max_tweets: Maximum returned tweets
        max_query_length: Maximum length allowed for a query on the Twitter API
        query_start_at: Index of first query from query cache to start the batch_scrape
        api_call_limit: Maximum number of API calls over the rate_limit_window
        rate_limit_window: Time window over which API calls are counted
        api_sleep_time: Sleep time between API calls if there is no pause

        api_call_times: A doubly ended queue store of timestamps for all API calls
        pause_indexes: A doubly ended queue store of timestamps indexes at which API fetching was paused

        tweet_fields: Tweet fields to return from API calls
        user_fields: User fields to return from API calls
        place_fields: Place fields to return from API calls
        expansions: Expanded fields to return from API calls (beyond standard API response)

        fetch_logging: Boolean to turn logging on and off
        dtformat: String format for time values

        seven_days: Boolean flag indicating if search is over standard 7 days or custom period

    '''

    # Data files paths
    query_file_path = "../../data/xtwitter/queries"
    tweets_file_path = "../../data/xtwitter/tweets"
    logs_file_path = "../../data/xtwitter/logs"
    keywords_file_path = "../../data/keywords"

    # Input files
    hashtags_file_name = "hashtags_other.csv"
    keywords_file_name = "keywords.csv"

    # Search qualifiers
    search_qualifiers = ["lang:en", "-is:retweet"]

    ### setting up the config
    max_tweets = 100

    # Max query length allowed by Twitter API
    max_query_length = 512

    query_start_at = 0

    # API Rate limits
    api_call_limit = 15
    rate_limit_window = timedelta(minutes=15)

    # Sleep between API calls (seconds) if there's no pause
    api_sleep_time = 2.5

    # Store timestamps of all API calls
    api_call_times = deque()
    pause_indexes = deque()

    # API return fields
    tweet_fields = ["context_annotations", "public_metrics", "created_at",
                    "text", "source", "geo",]
    user_fields = ["name", "username", "location", "verified", "description",
                   "public_metrics",]
    place_fields = ["country", "geo", "name", "place_type"]
    expansions = ["author_id", "geo.place_id", "referenced_tweets.id"]

    # Logging flag
    fetch_logging = True

    # Date format
    dtformat = "%Y-%m-%d %H:%M:%S"

    # Search window flag
    seven_days = True

    def __init__(self,
                 bearer_token,
                 consumer_key,
                 consumer_secret,
                 access_token,
                 access_token_secret,
                 start_time=None,
                 end_time=None,
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

        # Set start and end times if not None
        if type(start_time) != type(None) or type(end_time) != type(None):
            api_dtformat = "%Y-%m-%dT%H:%M:%SZ"
            self.start_time = start_time.strftime(api_dtformat)
            self.end_time = end_time.strftime(api_dtformat)

            self.seven_days = False
        
        # Turn off logging if needed
        if fetch_logging != True:
            self.fetch_logging = False


    def __create_query_cache(self):

        '''
        Method to create a cache of queries to use with the Twitter API. Note that the
        resulting queries will be in this format:
            (hashtag or other key search term) (keyword1 OR keyword2) lang:en -is:retweet'

        For example,
            (#victoriabc) (housing insecure OR encampment OR homeless OR homelessness OR housing
            OR shelter OR unhoused OR violence OR home OR affordable housing OR drugs OR evict
            OR poor OR poverty OR social housing OR substance use OR tent OR addict/addicted OR
            affordable OR alcoholic OR camp OR camper OR camping OR collude OR crime OR low-income
            OR narcotics OR overdose OR social problem OR social structure OR stolen OR theft OR thief)
            lang:en -is:retweet'

        Key parameters
            max_query_length: Maximum length allowed for a query on the Twitter API
            keywords_file_path: Location of the files containing hashtags and other search terms

        '''

        def split_keywords(kwlist: list,
                           join_str: str,
                           max_len: int):
            '''
            Function to split keywords into sublists that will
            keep the API search queries under the maximum length.

            Inputs:
                kwlist: list
                    List of keyword search terms
                join_str: str
                    The string that will join these terms in the search query (e.g. " OR ")
                max_len: int
                    The maximum length of this search phrase (e.g. Max Query length - less other phrases)

            '''

            join_len = len(join_str)

            # Initial values
            length = 0
            this_list = []

            # Loop through keywords counting length
            # Once it's loinger than max yield the list
            for kw in kwlist:
                length = length + len(kw) + join_len
                if length < max_len:
                    this_list.append(kw)
                else:
                    yield this_list
                    length = 0
                    this_list = [kw]

            if this_list == kwlist:
                yield kwlist

        try:

            # Read the hashtags and other file and create a list of hashtags and other major search terms
            df = pd.read_csv(os.path.join(self.keywords_file_path, self.hashtags_file_name), index_col=0)
            hashtags = df[df.columns[0]].str.strip().str.lower().unique().tolist()

            # Read the keywords file and create a list of keywords
            df = pd.read_csv(os.path.join(self.keywords_file_path, self.keywords_file_name), index_col=0)
            keywords = df[df.columns[0]].str.strip().str.lower().unique().tolist()

            # Create a phrase for extra search qualifiers beyond search terms
            qualstr = " ".join(self.search_qualifiers)

            # Create sublists
            keyword_splits = [k for k in split_keywords(kwlist=keywords,
                                                        join_str=" OR",
                                                        max_len=self.max_query_length - len(qualstr))]
            # Create the query cache
            self.query_cache = []
            for ht in hashtags:
                for kws in keyword_splits:
                    kwstr = " OR ".join([k.lower().strip() for k in kws])
                    q = "({}) ({}) {}".format(ht,
                                              kwstr,
                                              qualstr)

                    # remove unwanted characters
                    q = q.replace("&", "")
                    q = q.replace(" and ", ' "and" ')

                    # print(len(q))
                    self.query_cache.append((q, ht))

            # Log file found
            self.__log_event(msg_id=1, screen_print=False, event='query cache created',
                             query_cnt=len(self.query_cache))

        except:
            self.__log_event(msg_id=1, screen_print=True, event='query cache creation failed')

            msg = ("Query cache creation failed")
            raise RuntimeError(msg)


    def query_twitter(self,
                      search_query, 
                      search_hashtag_other):
        """
        Method to run one query against the API and store it
        """

        return_data = []

        # Determine what start times to pass to the API
        if self.seven_days:
            start_time = None
            end_time = None

        else:
            start_time = self.start_time
            end_time = self.end_time

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

        # Manage API call rate
        self.__manage_api_call_rate()

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

            ### unique ID
            newtweet["tweet_id"] = tweet.id

            # post time
            newtweet["created_at"] = str(tweet.created_at)

            # original text
            newtweet["text"] = tweet.text

            ### scrape time
            newtweet["scrape_time"] = str(datetime.now())

            ### working on quote tweets:
            if tweet.referenced_tweets:
                # print(tweet.text)
                for thist in tweet.referenced_tweets:
                    if thist.data["type"] == "quoted":
                        qt = self.client.get_tweet(thist.data["id"], tweet_fields=["text"])

                        # Add this to the count of API calls
                        self.__manage_api_call_rate()

                        mergetweet = (
                                newtweet["text"].strip() + " " + qt.data["text"].strip()
                        )
                        mergetweet = mergetweet.replace("\n", "")

                        newtweet["text"] = mergetweet

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
            newtweet["query"] = search_query

            ### more meta xtwitter
            newtweet["search_hashtag_other"] = search_hashtag_other

            return_data.append(newtweet)

        return return_data


    def batch_scrape(self):
        '''
        Method to run a batch of API queries calling the query_twitter method
        each time.

        Queries are loaded from the query_cache_file found at the query_file_path.

        '''

        # Start logger
        self.__log_event(msg_id=0, screen_print=False, logfile_stub='xtwitter')

        # Log fetch start
        self.__log_event(msg_id=1, screen_print=True, event='start fetch', source='xtwitter')

        # Create list of queries
        self.__create_query_cache()
        our_queries = self.query_cache[self.query_start_at:]

        try:
            # Set history file name
            history_file = "xtwitter_tweets.csv"

            # History file found
            history_df = pd.read_csv(os.path.join(self.tweets_file_path, history_file))
            history_cnt = len(history_df)

            # Get list  of tweet ids
            history_tweet_ids = history_df['tweet_id'].unique().tolist()

            # Log file found
            self.__log_event(msg_id=1, screen_print=False, event='history file loaded',
                             history_row_cnt=history_cnt)

        except:
            # No history file found
            history_cnt = 0
            history_df = None
            history_tweet_ids = []

            # Log file not found
            self.__log_event(msg_id=1, screen_print=False, event='No history file found', history_row_cnt=history_cnt)


        data_found = False
        for q in our_queries:

            try:
                # Get data from API
                data = self.query_twitter(search_query=q[0],
                                          search_hashtag_other=q[1])

                ### save our xtwitter - only if we got any
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
                    self.__log_event(msg_id=1, screen_print=False, event='fetch success',
                                     query_num=len(self.api_call_times), len_responses=len(final_results), query=q[1])

                else:

                    # Log no data returned
                    self.__log_event(msg_id=1, screen_print=False, event='fetch no data', query=q[1])

            except Exception as e:

                # Log exception
                self.__log_event(msg_id=1, screen_print=True, event='fetch exception',
                                 exception_info=str(e), query_num=len(self.api_call_times), query=q[0])

                # Reset  call limit
                self.api_call_times = len(self.api_call_times)

                # Manage API call rate
                self.__manage_api_call_rate()

                # Try to continue
                continue

        # Write new file
        if type(history_df) != type(None):
            new_data_df = pd.concat(objs=[history_df, final_results])

        elif type(final_results) != type(None):
            new_data_df = final_results

        else:
            self.__log_event(msg_id=1, screen_print=False, event='no results found')

        # Drop duplicates
        new_data_df = new_data_df.drop_duplicates(subset=['tweet_id'])

        # Save the file
        new_data_df.to_csv(path_or_buf=os.path.join(self.tweets_file_path, history_file),
                           index=False)

        # Log file update - finished fetch
        self.__log_event(msg_id=1, screen_print=True, event='fetch complete')

        # Close logging
        self.__log_event(msg_id=-1, screen_print=False)

    def __manage_api_call_rate(self):
        '''
        Method to pause fetch methods from calling the Reddit API to avoid rate limit exceptions.
        Pauses are only needed if the program has made too many API calls within a specified window.
        However. this method should be called after each API call since it also provides the
        check determining if a pause is needed.

        Key class parameters:
            self.api_call_limit: The maximum number of API calls during a specified time window
            self.rate_limit_window: The time window over which API calls are counted
            self.api_call_times = deque(): A doubly ended queue store of timestamps for all API calls
            self.pause_indexes = deque(): A doubly ended queue store of timestamps indexes at which API fetching
                    was paused


        '''

        # Record the time this method was called which should correspond with an API call time
        self.api_call_times.append(datetime.now())

        # Check rate limit - if equal to a multiple of the rate limit then we need to pause
        if len(self.api_call_times) >= self.api_call_limit * (len(self.pause_indexes) + 1):

            # Add this pause to the indexes of pauses
            self.pause_indexes.append(len(self.api_call_times) - 1)

            # If this is the first pause wait until time uses first API entry
            if len(self.pause_indexes) == 0:

                wait_until = self.api_call_times[0] + self.rate_limit_window

            # otherwise use the time of the last pause
            else:
                wait_until = self.api_call_times[self.pause_indexes[-1]] + self.rate_limit_window

            # Now wait by pausing the API
            wait_time = (wait_until - datetime.now()).total_seconds()

            #  Log the pause
            self.__log_event(msg_id=1, screen_print=True, event='rate limit reached',
                             wait_time_sec=wait_time, api_call_count=len(self.api_call_times))

            time.sleep(wait_time)

        else:

            # Even if we're not at limit's wait a short time
            wait_time = self.api_sleep_time
            time.sleep(wait_time)


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

