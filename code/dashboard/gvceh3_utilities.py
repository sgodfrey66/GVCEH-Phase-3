# Utilities supporting the GVCEH 3 Streamlit dashboard

import pandas as pd


class DashboardData:
    '''
    Class containing data to display on the Streamlit dashboard; currently
    Reddit and X (Twitter) data.


    '''

    # GCP project
    gcp_project_id = "npaicivitas"
    gcp_bucket_name = "gvceh-03a-storage"

    # Storage locations
    reddit_posts_path = "reddit/posts/"
    reddit_posts_file = "reddit_posts_scored.csv"

    xtwitter_tweets_path = "xtwitter/tweets/"
    xtwitter_tweets_file = "xtwitter_tweets_scored.csv"

    # Columns to check for duplicates
    reddit_dup_cols = ["id", "title", "selftext"]
    xtwitter_dup_cols = ["tweet_id", "created_at", "text"]

    dtformat = "%Y-%m-%d %H:%M:%S"
    dtformat_c = "%Y-%m-%d"

    def __init__(self):
        '''
        Initialize object

        '''

        # Read data
        self.__read_data__()

        # Calculate statistics
        self.__calculate_stats__()

        # Create tool tips
        self.__create_tooltips__()


    def __set_gcp_creds(self):
        '''
        Method to set GCP credentials
:
        '''

        # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "../../data/environment/parliament-rss-2177945b12d6.json"


    def __read_data__(self):
        '''
        Method to read data

        Input:
            data_source: str
                Flag indicating which data source to read

        '''

        # Read Reddit data

        # Reddit posts data
        posts_file_path = "gs://{}/{}{}".format(self.gcp_bucket_name,
                                                self.reddit_posts_path,
                                                self.reddit_posts_file)
        # Posts dataframe
        self.df_r = pd.read_csv(filepath_or_buffer=posts_file_path)

        # Drop duplicates
        self.df_r = self.df_r.drop_duplicates(subset=self.reddit_dup_cols)

        # Read Twitter data

        # Twitter tweets data
        tweets_file_path = "gs://{}/{}{}".format(self.gcp_bucket_name,
                                                 self.xtwitter_tweets_path,
                                                 self.xtwitter_tweets_file)

        self.df_x = pd.read_csv(filepath_or_buffer=tweets_file_path)

        # Drop duplicates
        self.df_x = self.df_x.drop_duplicates(subset=self.xtwitter_dup_cols)

        # self.dsource_dict = {"Twitter": self.df_x,
        #                      "Reddit": self.df_r}

    def __create_tooltips__(self):
        '''
            Method to create a dictionary of tooltips
        '''
        readme = {}
        readme['data_source'] = 'Would you like to see an analysis for Twitter or Reddit?'
        readme[
            'prior_period'] = 'A prior period comparison enables comparison of results for this period against the previous period. e.g. the prior period for 4/10/22 - 4/16/22 would be 4/3/22 - 4/9/22.'
        readme[
            'top_influencers'] = 'The top influencers for a time period are calculated using a weighted measure of number of tweets, reply and retweet count, like count, number of followers and an influencer flag based on the appendix.'

        self.readme = readme

    def __calculate_stats__(self):
        '''
        Method to calculate dashboard statistics

        :return:
        '''

        #### Calculate Reddit stats

        # Get activity statistics
        maskr1 = self.df_r["is_relevant"] == 1
        self.reddit_post_cnt = self.df_r[maskr1]["id"].nunique()
        self.reddit_user_cnt = self.df_r[maskr1]["author"].nunique()
        self.reddit_subreddit_cnt = self.df_r[maskr1]["subreddit"].nunique()

        r_sent_val_cnts = self.df_r[maskr1]["sentiment"].value_counts()
        self.reddit_pos_rate = r_sent_val_cnts["positive"] / self.reddit_post_cnt
        self.reddit_neg_rate = r_sent_val_cnts["negative"] / self.reddit_post_cnt
        self.reddit_neu_rate = r_sent_val_cnts["neutral"] / self.reddit_post_cnt

        # Convert string column to dates
        reddit_times = pd.to_datetime(self.df_r["created_at"])

        # Get starting and ending dates
        self.reddit_start = reddit_times.min().strftime(self.dtformat_c)
        self.reddit_end = reddit_times.max().strftime(self.dtformat_c)

        #### Calculate Twitter stats

        # Get activity statistics
        maskx1 = self.df_x["is_relevant"] == 1
        self.xtwitter_tweet_cnt = self.df_x[maskx1]["tweet_id"].nunique()
        self.xtwitter_user_cnt = self.df_x[maskx1]["username"].nunique()
        self.xtwitter_location_cnt = self.df_x[maskx1]["user_location"].nunique()

        # Get sentiment rates
        x_sent_val_cnts = self.df_x[maskx1]["sentiment"].value_counts()
        self.xtwitter_pos_rate = x_sent_val_cnts["positive"] / self.xtwitter_tweet_cnt
        self.xtwitter_neg_rate = x_sent_val_cnts["negative"] / self.xtwitter_tweet_cnt
        self.xtwitter_neu_rate = x_sent_val_cnts["neutral"] / self.xtwitter_tweet_cnt

        # Convert column to date
        xtwitter_times = pd.to_datetime(self.df_x["created_at"])

        # Get starting and ending dates
        self.xtwitter_start = xtwitter_times.min().strftime(self.dtformat_c)
        self.xtwitter_end = xtwitter_times.max().strftime(self.dtformat_c)
