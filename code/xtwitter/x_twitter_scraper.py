# Core Python
import os

# Environment variables for API credential storage
import dotenv

# GVCEH objects
import x_twitter_data_fetcher as xtdf


if __name__ == "__main__":

    '''
    This function uses the GVCEHReddit class to retrieve 
    Reddit from specific Subreddits by searching for specific 
    keyword or search terms.
    
    See GVCEHReddit documentation for details on inputs and outputs.
    
    '''

    # import out environment variables
    dotenv.load_dotenv('../../data/environment/.env')


    # Initialize GVCEHXTwitter object
    data_fetcher = xtdf.GVCEHXTwitter(bearer_token=os.environ.get("TWITTER_BEARER_TOKEN"),
                                      consumer_key=os.environ.get("TWITTER_CONSUMER_KEY"),
                                      consumer_secret=os.environ.get("TWITTER_CONSUMER_SECRET"),
                                      access_token=os.environ.get("TWITTER_ACCESS_TOKEN"),
                                      access_token_secret=os.environ.get("TWITTER_ACCESS_TOKEN_SECRET"))

    # Fetch Twitter data
    data_fetcher.batch_scrape()


