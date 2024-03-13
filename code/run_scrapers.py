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

    # GCP project
    project_id = "597122211821"

    # ID and version of secret
    secret_id = "REDDIT_CLIENT_ID"
    version_id = "1"

    # Update user
    print('Collecting Reddit data ')

    # Step 1: Initialize GVCEHReddit object
    # data_fetcher = rdf.GVCEHReddit(client_id=os.environ.get("REDDIT_CLIENT_ID"),
    #                                client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
    #                                user_agent=os.environ.get("REDDIT_USER_AGENT"))

    data_fetcher = rdf.GVCEHReddit(client_id=get_gcpsecrets(project_id, "REDDIT_CLIENT_ID", version_id),
                                   client_secret=get_gcpsecrets(project_id, "REDDIT_CLIENT_SECRET", version_id),
                                   user_agent=get_gcpsecrets(project_id, "REDDIT_USER_AGENT", version_id),
                                   posts_file_path="../data/reddit/posts",
                                   logs_file_path="../data/reddit/logs",
                                   keywords_file_path="../data/keywords")

    # Subreddits to explore
    subreddit_names = ["OakBayBritishColumbia", "SaanichPeninsula", "britishcolumbia",
                       "Sooke", "Esquimalt", "SidneyBC", "saltspring",
                       "Metchosin", "WestShoreBC", "VancouverIsland", "uvic"]

    # Step 2: Fetch reddit data
    asyncio.run(data_fetcher.fetch_search_data(subreddit_names=subreddit_names))
    # asyncio.run(data_fetcher.fetch_new_data(subreddit_names=subreddit_names))

    # Update user
    print('Collecting X (Twitter) data ')

    # Step 3: Initialize GVCEHXTwitter object
    # data_fetcher = xtdf.GVCEHXTwitter(bearer_token=os.environ.get("TWITTER_BEARER_TOKEN"),
    #                                   consumer_key=os.environ.get("TWITTER_CONSUMER_KEY"),
    #                                   consumer_secret=os.environ.get("TWITTER_CONSUMER_SECRET"),
    #                                   access_token=os.environ.get("TWITTER_ACCESS_TOKEN"),
    #                                   access_token_secret=os.environ.get("TWITTER_ACCESS_TOKEN_SECRET"))

    data_fetcher = xtdf.GVCEHXTwitter(bearer_token=get_gcpsecrets(project_id,"TWITTER_BEARER_TOKEN", version_id),
                                      consumer_key=get_gcpsecrets(project_id,"TWITTER_CONSUMER_KEY", version_id),
                                      consumer_secret=get_gcpsecrets(project_id,"TWITTER_CONSUMER_SECRET", version_id),
                                      access_token=get_gcpsecrets(project_id,"TWITTER_ACCESS_TOKEN", version_id),
                                      access_token_secret=get_gcpsecrets(project_id,"TWITTER_ACCESS_TOKEN_SECRET", version_id),
                                      query_file_path="../data/xtwitter/queries",
                                      tweets_file_path="../data/xtwitter/tweets",
                                      logs_file_path="../data/xtwitter/logs",
                                      keywords_file_path="../data/keywords")

    # Step 4: Fetch Twitter data
    data_fetcher.batch_scrape()

    print('Scrapers run complete')




