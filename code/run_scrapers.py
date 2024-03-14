# Core Python
import os, sys

# Environment variables for API credential storage
import dotenv

# Asynchronous work
import asyncio

# GCP
from google.cloud import secretmanager

# GVCEH objects
sys.path.insert(0, "reddit/")
import reddit_data_fetcher as rdf


# GVCEH objectscl
sys.path.insert(0, "xtwitter/")
import x_twitter_data_fetcher as xtdf



def get_gcpsecrets(project_id,
                   secret_id,
                   version_id="latest"):
    """
    Access a secret version in Google Cloud Secret Manager.

    Args:
        project_id: GCP project ID.
        secret_id: ID of the secret you want to access.
        version_id: Version of the secret (defaults to "latest").

    Returns:
        The secret value as a string.
    """
    # Create the Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = client.access_secret_version(request={"name": name})

    # Return the payload as a string
    # Note: response.payload.data is a bytes object, decode it to a string
    return response.payload.data.decode("UTF-8")



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
        xtwitter_tweets_file_path = "../data/xtwitter/tweets",
        xtwitter_logs_file_path = "../data/xtwitter/logs"

        keywords_file_path = "../data/keywords"

    else:

        # GCP project
        project_id = "597122211821"
        # Version of secret
        version_id = "1"

        # Reddit credentials
        REDDIT_CLIENT_ID = get_gcpsecrets(project_id, "REDDIT_CLIENT_ID", version_id)
        REDDIT_CLIENT_SECRET = get_gcpsecrets(project_id, "REDDIT_CLIENT_SECRET", version_id)
        REDDIT_USER_AGENT = get_gcpsecrets(project_id, "REDDIT_USER_AGENT", version_id)

        # XTwitter credentials
        TWITTER_BEARER_TOKEN = get_gcpsecrets(project_id, "TWITTER_BEARER_TOKEN", version_id)
        TWITTER_CONSUMER_KEY = get_gcpsecrets(project_id, "TWITTER_CONSUMER_KEY", version_id)
        TWITTER_CONSUMER_SECRET = get_gcpsecrets(project_id, "TWITTER_CONSUMER_SECRET", version_id)
        TWITTER_ACCESS_TOKEN = get_gcpsecrets(project_id, "TWITTER_ACCESS_TOKEN", version_id)
        TWITTER_ACCESS_TOKEN_SECRET = get_gcpsecrets(project_id, "TWITTER_ACCESS_TOKEN_SECRET", version_id)

        # Google cloud storage credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = get_gcpsecrets(project_id, "GOOGLE_APPLICATION_CREDENTIALS", version_id)

        # File locations
        bucket_name = "gvceh-03a-storage"
        bucket_path = "gs://{}".format(bucket_name)
        reddit_posts_file_path = "{}/reddit/posts".format(bucket_path)
        reddit_logs_file_path = "{}/reddit/logs".format(bucket_path)
        ###########
        reddit_logs_file_path = "../data/reddit/logs"
        xtwitter_tweets_file_path = "{}/xtwitter/tweets".format(bucket_path)
        xtwitter_logs_file_path = "{}/xtwitter/logs".format(bucket_path)
        ###########
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

    # Subreddits to explore
    subreddit_names = ["OakBayBritishColumbia", "SaanichPeninsula", "britishcolumbia",
                       "Sooke", "Esquimalt", "SidneyBC", "saltspring",
                       "Metchosin", "WestShoreBC", "VancouverIsland", "uvic"]

    subreddit_names = ["britishcolumbia"]

    # Step 3: Fetch reddit data
    asyncio.run(data_fetcher.fetch_search_data(subreddit_names=subreddit_names))
    # asyncio.run(data_fetcher.fetch_new_data(subreddit_names=subreddit_names))

    # Update user
    print('Collecting X (Twitter) data ')

    # Step 4: Initialize GVCEHXTwitter object
    data_fetcher = xtdf.GVCEHXTwitter(bearer_token=TWITTER_BEARER_TOKEN,
                                      consumer_key=TWITTER_CONSUMER_KEY,
                                      consumer_secret=TWITTER_CONSUMER_SECRET,
                                      access_token=TWITTER_ACCESS_TOKEN,
                                      access_token_secret=TWITTER_ACCESS_TOKEN_SECRET,
                                      tweets_file_path=xtwitter_tweets_file_path,
                                      logs_file_path=xtwitter_logs_file_path,
                                      keywords_file_path=keywords_file_path)


    # Step 5: Fetch Twitter data
    data_fetcher.batch_scrape()

    print('Scrapers run complete')




