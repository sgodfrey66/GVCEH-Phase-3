# Core python
import os
import re
from collections import deque

# import praw
import asyncpraw

# Data
import pandas as pd

# Time
from datetime import datetime, timedelta
import time

# Logging and monitoring
import logging


class GVCEHReddit():
    '''
    Class to handle Reddit API calls for the GVCEH project.  Most of the
    work is done through the fetch_data method which retrieves data from
    Reddit Subreddits using the AsyncPraw library's search tool.

    Search terms are loaded by reading files with keywords.  This tool
    maintains a search history and only adds new submissions if they are
    not already in the search history.

    Primary output are the post files found in the posts file path.  The
    naming convention for these files is {subreddit}_posts_data.csv.  Note
    that the logic within the fetch_data excludes any submission IDs already
    in the posts output file.

    The fetch_data method has an optional logging feature which can be turned on
    by setting the logging input parameter to True.

    References:

        Async PRAW: https://asyncpraw.readthedocs.io/en/stable/

        For Reddit API details and rate limit rules,
        see https://support.reddithelp.com/hc/en-us/articles/16160319875092-Reddit-Data-API-Wiki.
        As of Feb 2024, 100 queries per minute over a 10-minute window.

        Although not used here, this might prove helpful: https://reddit-api.readthedocs.io/en/latest/#.

    Inputs:
        __init__ :
            client_id: Reddit API ID
            client_secret: Reddit client secret
            user_agent: Reddit API user agent
            fetch_type: Flag to indicate if data fetch should be a search or all new posts
            fetch_logging: True if logging should be one

    Outputs:
        fetch_data:
            Posts files found in the posts_file_path
            Log files found in the logs_file_path

    Attributes:
        posts_file_path: Path to the retrieved posts or submissions
        logs_file_path: Path to the logs captured during retrieval
        keywords_file_path: Path to the CSV files with keyword search terms

        df_columns: Columns of the retrieved submissions data
        keywords_files:  Names of files with keywords

        api_call_limit: Maximum number of calls to the API within the rate limit time window
        rate_limit_window: Time window over which API calls are counted
        search_time_filter:  The look-back period for the API search
        limit_num: Maximum number of submissions to retrieve in response to a search
        file_update_trigger:  Number of submissions to retrieve before updating output data

        api_call_times: A doubly ended queue store of timestamps for all API calls
        pause_indexes: A doubly ended queue store of timestamps indexes at which API fetching was paused

        fetch_logging: Boolean to turn logging on and off
        dtformat: String format for time values

    '''

    # Data files paths
    posts_file_path = "../../data/reddit/posts"
    logs_file_path = "../../data/reddit/logs"
    keywords_file_path = "../../data/keywords"

    # Reddit submission attributes to retain
    df_columns = ["id", "created_at", "scrape_time", "author", "subreddit", "title",
                  "selftext", "url", "num_comments", "search_term"]

    # Files with keywords used to search reddits
    # keywords_files = ['ac.csv', 'ad.csv', 'ae.csv']
    keywords_files = ["keywords.csv", "hashtags_other.csv"]

    # API Rate limits
    # api_call_limit = 880
    api_call_limit = 440
    rate_limit_window = timedelta(minutes=10)

    # Sleep between API calls (seconds) if there's no pause
    api_sleep_time = 0.05

    # Max number of submissions to retrieve
    limit_num = 1000
    
    # New limit number - maximum posts to retrieve using fetch_new endpoint
    new_limit_num = 125

    # Number of new rows triggering a file update
    file_update_trigger = 500

    # Store timestamps of all API calls
    api_call_times = deque()
    pause_indexes = deque()

    # Fetch search lookback window
    search_time_filter = "month"

    # Logging flag
    fetch_logging = True

    # Date format
    dtformat = "%Y-%m-%d %H:%M:%S"


    def __init__(self,
                 client_id,
                 client_secret,
                 user_agent,
                 fetch_logging=True,
                 **kwargs
                 ):
        '''
        Initialize the GVCEHReddit class.
        '''

        # Update any key word args
        self.__dict__.update(kwargs)

        # Assign API values for use later
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent

        # Turn off logging if needed
        if fetch_logging != True:
            self.fetch_logging = False

    def __get_search_terms(self):
        '''
        Get search terms from files with keywords
        '''

        # A list holding keywords
        keywords = []

        # read each file with keywords
        for kwf in self.keywords_files:

            # Read the files into a dataframe
            df_kw = pd.read_csv(os.path.join(self.keywords_file_path, kwf), index_col=0)

            # Get the cleaned keywords
            keywords.extend([self.__clean_keyword_text(k) for k in df_kw[df_kw.columns[0]].tolist()])

        # Ensure keywords are strings and remove any duplicates
        self.search_terms = [k for k in set(keywords) if isinstance(k, str) and len(k) > 1]


    async def fetch_search_data(self,
                                subreddit_names: list):

        '''
        Function for retrieving Reddit data using the search method.

        This function needs to be called with await
        # df = await test.fetch_data(subreddit_names)

        '''

        # Get the search terms
        self.__get_search_terms()

        # Initialize a asyncpraw reddit object
        reddit = asyncpraw.Reddit(client_id=self.client_id,
                                  client_secret=self.client_secret,
                                  user_agent=self.user_agent)

        # Search in each subreddit
        for subreddit_name in subreddit_names:

            # Create a subreddit class
            subreddit = await reddit.subreddit(subreddit_name)

            # Get history file name, if it exists
            history_file = f'{subreddit_name}_posts_data.csv'

            # Start logger
            self.__log_event(msg_id=0, screen_print=False, logfile_stub=subreddit_name)

            # Log fetch start
            self.__log_event(msg_id=1, screen_print=True, event='start fetch', subreddit_name=subreddit_name)

            # Read files with previous Reddit data into a dataframe
            try:
                subreddit_df = pd.read_csv(os.path.join(self.posts_file_path, history_file))
                seen_submission_ids = set(subreddit_df['id'])

                # check if history columns match expected
                if subreddit_df.columns != self.df_columns:

                    # Log submission processing
                    msg = ("History file appears corrupted")
                    self.__log_event(msg_id=1, screen_print=True, event='processing error', error_msg=msg)

                    raise RuntimeError(msg)

                # Initialize last written index
                last_written_index = subreddit_df.index.max()

            except FileNotFoundError:
                # Create a new dataframe
                subreddit_df = pd.DataFrame(columns=self.df_columns)
                seen_submission_ids = set()

                # Initialize last written index
                last_written_index = -1

            # Now search for search terms
            for search_term in self.search_terms:

                # Search Reddit for each search term
                try:

                    async for submission in subreddit.search(search_term,
                                                             limit=self.limit_num,
                                                             time_filter=self.search_time_filter):

                        # Manage API call rate
                        self.__manage_api_call_rate()

                        # Check if we already have this submission in the dataset
                        if submission.id in seen_submission_ids:

                            # Log submission found
                            self.__log_event(msg_id=1, screen_print=False, event='submission ID found', id=submission.id)

                            continue

                        # Log submission processing
                        self.__log_event(msg_id=1, screen_print=False, event='submission processing', id=submission.id)

                        # Load data for this submission id
                        await submission.load()
                        seen_submission_ids.add(submission.id)

                        # Manage API call rate
                        self.__manage_api_call_rate()

                        # Dictionary to hold
                        sub_dict = {}

                        # Add submission to dictionary
                        for col in self.df_columns:

                            if col == "created_at":
                                sub_dict[col] = datetime.utcfromtimestamp(int(getattr(submission, "created_utc")))

                            elif col == "scrape_time":
                                sub_dict[col] =  datetime.now().strftime(self.dtformat)

                            elif col == "search_term":
                                sub_dict[col] = search_term

                            else:
                                sub_dict[col] = [getattr(submission, col)]


                        # Create a dataframe
                        new_data = pd.DataFrame(sub_dict)

                        # Concatenate with previous data unless first entries
                        if len(subreddit_df) == 0:
                            subreddit_df = new_data
                        else:
                            subreddit_df = pd.concat([subreddit_df, new_data], ignore_index=True)

                        if len(subreddit_df) >= last_written_index + self.file_update_trigger:

                            # Determine new rows to be added
                            new_rows = subreddit_df.iloc[last_written_index + 1:
                                                         last_written_index + self.file_update_trigger + 1]

                            # Add rows to history file
                            new_rows.to_csv(os.path.join(self.posts_file_path, history_file),
                                            mode='a',
                                            index=False,
                                            header=last_written_index == -1)

                            last_written_index += len(new_rows)  # Update the last written index

                            # Log file update
                            self.__log_event(msg_id=1, screen_print=False, event='append data to result file',
                                             last_written_index=last_written_index, new_row_count=len(subreddit_df))

                except Exception as e:

                    # Log exception
                    self.__log_event(msg_id=1, screen_print=True, event='fetch exception',
                                     exception_info=str(e), query_num=len(self.api_call_times), search_term=search_term)

                    raise RuntimeError(e)

            # Final append for any remaining rows after the last intermittent save
            if len(subreddit_df) > last_written_index + 1:

                # Determine new rows to be added
                new_rows = subreddit_df.iloc[last_written_index + 1:]

                # Add rows to history file
                new_rows.to_csv(os.path.join(self.posts_file_path, history_file),
                                mode='a',
                                index=False,
                                header=last_written_index == -1)

                # Log file update
                self.__log_event(msg_id=1, screen_print=False, event='final data file append',
                                 last_written_index=last_written_index, new_row_count=len(subreddit_df))

        # Write the combined file
        self.__concat_posts_files(subreddit_names=subreddit_names)

        # Log file update - finished fetch
        self.__log_event(msg_id=1, screen_print=True, event='fetch complete')

        # Close logging
        self.__log_event(msg_id=-1, screen_print=False)

        # Close reddit object
        await reddit.close()

        # return subreddit_df


    async def fetch_new_data(self,
                             subreddit_names: list):

        '''
        Function for retrieving Reddit data using the new method.

        This function needs to be called with await
        # df = await test.fetch_data(subreddit_names)

        '''

        # Initialize a asyncpraw reddit object
        reddit = asyncpraw.Reddit(client_id=self.client_id,
                                  client_secret=self.client_secret,
                                  user_agent=self.user_agent)

        # Search in each subreddit
        for subreddit_name in subreddit_names:

            # Create a subreddit class
            subreddit = await reddit.subreddit(subreddit_name)

            # Get history file name, if it exists
            history_file = f'{subreddit_name}_posts_data.csv'

            # Start logger
            self.__log_event(msg_id=0, screen_print=False, logfile_stub=subreddit_name)

            # Log fetch start
            self.__log_event(msg_id=1, screen_print=True, event='start fetch', subreddit_name=subreddit_name)

            # Read files with previous Reddit data into a dataframe
            try:
                subreddit_df = pd.read_csv(os.path.join(self.posts_file_path, history_file))
                seen_submission_ids = set(subreddit_df['id'])

                # check if history columns match expected
                if subreddit_df.columns != self.df_columns:
                    # Log submission processing
                    msg = ("History file appears corrupted")
                    self.__log_event(msg_id=1, screen_print=True, event='processing error', error_msg=msg)

                    raise RuntimeError(msg)

                # Initialize last written index
                last_written_index = subreddit_df.index.max()

            except FileNotFoundError:
                # Create a new dataframe
                subreddit_df = pd.DataFrame(columns=self.df_columns)
                seen_submission_ids = set()

                # Initialize last written index
                last_written_index = -1

            # Manage API call rate
            self.__manage_api_call_rate()

            try:

                # async for submission in subreddit.new(limit=self.limit_num):
                async for submission in subreddit.new(limit=self.new_limit_num):

                    # Manage API call rate
                    self.__manage_api_call_rate()

                    # Check if we already have this submission in the dataset
                    if submission.id in seen_submission_ids:

                        # Log submission found
                        self.__log_event(msg_id=1, screen_print=False, event='submission ID found', id=submission.id)

                        continue

                    # Log submission processing
                    self.__log_event(msg_id=1, screen_print=False, event='submission processing', id=submission.id)

                    # Load data for this submission id
                    await submission.load()
                    seen_submission_ids.add(submission.id)

                    # Manage API call rate
                    self.__manage_api_call_rate()

                    # Dictionary to hold
                    sub_dict = {}

                    # Add submission to dictionary
                    for col in self.df_columns:

                        if col == "created_at":
                            sub_dict[col] = datetime.utcfromtimestamp(int(getattr(submission, "created_utc")))

                        elif col == "scrape_time":
                            sub_dict[col] = datetime.now().strftime(self.dtformat)

                        elif col == "search_term":
                            # Collect data for this search term --- Since new posts fetch set to all_new_posts
                            sub_dict[col] = "all_new_posts"

                        else:
                            sub_dict[col] = [getattr(submission, col)]

                    # Create a dataframe
                    new_data = pd.DataFrame(sub_dict)

                    # Concatenate with previous data unless first entries
                    if len(subreddit_df) == 0:
                        subreddit_df = new_data
                    else:
                        subreddit_df = pd.concat([subreddit_df, new_data], ignore_index=True)

                    if len(subreddit_df) >= last_written_index + self.file_update_trigger:

                        # Determine new rows to be added
                        new_rows = subreddit_df.iloc[last_written_index + 1:
                                                     last_written_index + self.file_update_trigger + 1]

                        # Add rows to history file
                        new_rows.to_csv(os.path.join(self.posts_file_path, history_file),
                                        mode='a',
                                        index=False,
                                        header=last_written_index == -1)

                        last_written_index += len(new_rows)  # Update the last written index

                        # Log file update
                        self.__log_event(msg_id=1, screen_print=False, event='append data to result file',
                                         last_written_index=last_written_index, new_row_count=len(subreddit_df))

            except Exception as e:

                # Log exception
                self.__log_event(msg_id=1, screen_print=True, event='fetch exception',
                                 exception_info=str(e), query_num=len(self.api_call_times), subreddit=subreddit_name)

                raise RuntimeError(e)

            # Final append for any remaining rows after the last intermittent save
            if len(subreddit_df) > last_written_index + 1:

                # Determine new rows to be added
                new_rows = subreddit_df.iloc[last_written_index + 1:]

                # Add rows to history file
                new_rows.to_csv(os.path.join(self.posts_file_path, history_file),
                                mode='a',
                                index=False,
                                header=last_written_index == -1)

                # Log file update
                self.__log_event(msg_id=1, screen_print=False, event='final data file append',
                                 last_written_index=last_written_index, new_row_count=len(subreddit_df))

        # Write the combined file
        self.__concat_posts_files(subreddit_names=subreddit_names)

        # Log file update - finished fetch
        self.__log_event(msg_id=1, screen_print=True, event='fetch complete')

        # Close logging
        self.__log_event(msg_id=-1, screen_print=False)

        # Close reddit object
        await reddit.close()


    def __concat_posts_files(self,
                             subreddit_names: list):
        '''
        Method to concatenate the posts into a single dataframe and file.

        '''

        # Name the file containing the combined reddit output
        combined_file_stub = "reddit_posts"
        combined_file_name = "{}.csv".format(combined_file_stub)

        # Log beginning file concatenation process
        self.__log_event(msg_id=0, screen_print=False, event='starting concatenation process',
                         logfile_stub=combined_file_stub)

        dfs = []
        for subreddit_name in subreddit_names:

            # Get the name of the history file
            history_file = f'{subreddit_name}_posts_data.csv'

            try:
                # read the file
                df = pd.read_csv(filepath_or_buffer=os.path.join(self.posts_file_path, history_file))

                # Add to list
                dfs.append(df)

                # Log successful file read
                self.__log_event(msg_id=1, screen_print=False, event='successful history file read',
                                 file_name=history_file)

            except:

                # Log unsuccessful file read
                self.__log_event(msg_id=1, screen_print=False, event='unsuccessful history file read',
                                 file_name=history_file)

        # Concatenate
        df = pd.concat(objs=dfs)
        df = df.reset_index(drop=True)

        # Write combined file
        df.to_csv(path_or_buf=os.path.join(self.posts_file_path, combined_file_name),
                  index=False)

        # Log successful file save
        self.__log_event(msg_id=1, screen_print=False, event='successful history file save',
                         file_name=combined_file_name)


    def __clean_keyword_text(self,
                           text):
        '''
        Method to clean Reddit and keyword texts in either the original post
        or associated comments.

        '''

        # Remove these words from search terms
        stop_words = ['stop', 'the', 'to', 'and', 'a', 'in', 'it',
                      'is', 'I', 'i', 'that', 'had', 'on', 'for', 'were', 'was',
                      'through', 'of', 'way', 'end', 'our', 'place', 'home',
                      'support', 'city', 'visitor', 'women', 'men', 'need', 'idea',
                      'north', 'south', 'east', 'west', 'ready', 'save', 'salt', 'win',
                      'lose', 'loss', 'family', 'working', 'hope', 'love', 'house']

        # Remove unwanted characters
        pat = r"\\n|r/|[^a-zA-Z0-9 ]"
        text = re.sub(pat, '', text)

        # cohvert to lowercase
        text = text.strip().lower()

        # Remove stop words
        pat = "|".join(["\\b{}\\b".format(w) for w in stop_words])
        text = re.sub(pat, ' ', text)

        # remove extra spaces
        pat = r"\s{2,}"
        text = re.sub(pat, ' ', text)

        # Remove leading and trailing spaces
        text = text.strip()

        return text


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

        # Get current time
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
            # print('log_path: {}'.format(self.logs_file_path))
            # print('log_file_path: {}'.format(os.path.join(self.logs_file_path, log_file)))
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

