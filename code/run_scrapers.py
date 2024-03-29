##############
# File to run the Reddit and X scrapers
# and then score tweets/posts for relevance and sentiment
# Steve Godfrey
# Mar 2024
#
##############

# Core Python
import os, sys

# Environment variables for API credential storage
import dotenv

# Asynchronous work
import asyncio



# GVCEH objects
sys.path.insert(0, "reddit/")
import reddit_data_fetcher as rdf
import reddit_scorer as rds

# GVCEH objectscl
sys.path.insert(0, "xtwitter/")
import x_twitter_data_fetcher as xtdf
import x_twitter_scorer as xts

# GVCEH objectscl
sys.path.insert(0, "utils/")
import gcp_tools as gt

if __name__ == "__main__":

    '''
    This function uses the GVCEHReddit class to retrieve 
    Reddit from specific Subreddits by searching for specific 
    keyword or search terms.
    
    See GVCEHReddit documentation for details on inputs and outputs.
    
    '''

    # Step 1: Get environment variables
    # Default setting is cloud run meaning use GCP for environmental variables
    # And save to GCP storage - otherwise run locally and get environmental
    # variables from .env using dotenv and save locally

    # GCP project
    # project_num = "597122211821"
    project_id = "npaicivitas"

    # Version of GCP secret
    version_id = "1"


    if len(sys.argv) > 1 and sys.argv[1].lower() == "local":
        # Reddit credentials
        REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
        REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
        REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT")

        # XTwitter credentials
        TWITTER_BEARER_TOKEN = os.environ.get("TWITTER_BEARER_TOKEN")
        TWITTER_CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
        TWITTER_CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
        TWITTER_ACCESS_TOKEN = os.environ.get("TWITTER_ACCESS_TOKEN")
        TWITTER_ACCESS_TOKEN_SECRET = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")

        # File locations
        reddit_posts_file_path = "../data/reddit/posts"
        reddit_logs_file_path = "../data/reddit/logs"
        reddit_models_file_path ="../data/models/reddit"
        xtwitter_tweets_file_path = "../data/xtwitter/tweets"
        xtwitter_logs_file_path = "../data/xtwitter/logs"

        keywords_file_path = "../data/keywords"

    else:

        # Reddit credentials
        REDDIT_CLIENT_ID = gt.get_gcpsecrets(project_id, "REDDIT_CLIENT_ID", version_id)
        REDDIT_CLIENT_SECRET = gt.get_gcpsecrets(project_id, "REDDIT_CLIENT_SECRET", version_id)
        REDDIT_USER_AGENT = gt.get_gcpsecrets(project_id, "REDDIT_USER_AGENT", version_id)

        # XTwitter credentials
        TWITTER_BEARER_TOKEN = gt.get_gcpsecrets(project_id, "TWITTER_BEARER_TOKEN", version_id)
        TWITTER_CONSUMER_KEY = gt.get_gcpsecrets(project_id, "TWITTER_CONSUMER_KEY", version_id)
        TWITTER_CONSUMER_SECRET = gt.get_gcpsecrets(project_id, "TWITTER_CONSUMER_SECRET", version_id)
        TWITTER_ACCESS_TOKEN = gt.get_gcpsecrets(project_id, "TWITTER_ACCESS_TOKEN", version_id)
        TWITTER_ACCESS_TOKEN_SECRET = gt.get_gcpsecrets(project_id, "TWITTER_ACCESS_TOKEN_SECRET", version_id)

        # Google cloud storage credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gt.get_gcpsecrets(project_id, "GOOGLE_APPLICATION_CREDENTIALS", version_id)

        # File locations
        bucket_name = "gvceh-03a-storage"
        bucket_path = "gs://{}".format(bucket_name)

        reddit_posts_file_path = "{}/reddit/posts".format(bucket_path)
        # reddit_logs_file_path = "{}/reddit/logs".format(bucket_path)
        # reddit_models_file_path ="../data/models/reddit"
        reddit_models_file_path = "{}/reddit/models".format(bucket_path)
        reddit_logs_file_path = "../data/reddit/logs"

        xtwitter_tweets_file_path = "{}/xtwitter/tweets".format(bucket_path)
        # xtwitter_logs_file_path = "{}/xtwitter/logs".format(bucket_path)
        xtwitter_logs_file_path = "../data/xtwitter/logs"

        keywords_file_path = "{}/keywords".format(bucket_path)

    # Update user
    print('Collecting Reddit data ')

    # Step 2: Initialize GVCEHReddit object
    data_fetcher = rdf.GVCEHReddit(client_id=REDDIT_CLIENT_ID,
                                   client_secret=REDDIT_CLIENT_SECRET,
                                   user_agent=REDDIT_USER_AGENT,
                                   posts_file_path=reddit_posts_file_path,
                                   logs_file_path=reddit_logs_file_path,
                                   keywords_file_path=keywords_file_path)

    # Step 3: Fetch reddit data
    asyncio.run(data_fetcher.fetch_search_data())

    # Step 4. Score posts
    rds.ScorePosts(posts_file_path=reddit_posts_file_path,
                   logs_file_path=reddit_logs_file_path,
                   relevance_model_path=reddit_models_file_path,
                   gcp_project_id=project_id)

    # Update user
    print('Collecting X (Twitter) data ')

    # Step 5: Initialize GVCEHXTwitter object
    data_fetcher = xtdf.GVCEHXTwitter(bearer_token=TWITTER_BEARER_TOKEN,
                                      consumer_key=TWITTER_CONSUMER_KEY,
                                      consumer_secret=TWITTER_CONSUMER_SECRET,
                                      access_token=TWITTER_ACCESS_TOKEN,
                                      access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
                                      tweets_file_path=xtwitter_tweets_file_path,
                                      logs_file_path=xtwitter_logs_file_path,
                                      keywords_file_path=keywords_file_path)

    # Step 6: Fetch Twitter data
    data_fetcher.batch_scrape()

    # Step 7. Score tweets
    xts.ScoreTweets(tweets_file_path=xtwitter_tweets_file_path,
                    logs_file_path=xtwitter_logs_file_path,
                    gcp_project_id=project_id)

    print('Scrapers run complete')




