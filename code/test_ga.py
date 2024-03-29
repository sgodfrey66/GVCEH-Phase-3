# Core Python
import os, sys


# # Environment variables for API credential storage
# import dotenv
#
# # Asynchronous work
# import asyncio
#
# GVCEH objects
sys.path.insert(0, 'code/reddit/')
import reddit_data_fetcher as rdf

# GVCEH objects
sys.path.insert(0, 'code/xtwitter/')
import x_twitter_data_fetcher as xtdf

if __name__ == "__main__":
    '''
    This function uses the GVCEHReddit class to retrieve 
    Reddit from specific Subreddits by searching for specific 
    keyword or search terms.

    See GVCEHReddit documentation for details on inputs and outputs.

    '''

    # print(os.listdir('../'))
    print(os.listdir('data/reddit/posts'))

    data_fetcher = rdf.GVCEHReddit(client_id=os.environ.get("REDDIT_CLIENT_ID"),
                                   client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
                                   user_agent=os.environ.get("REDDIT_USER_AGENT"))

    print('hello')