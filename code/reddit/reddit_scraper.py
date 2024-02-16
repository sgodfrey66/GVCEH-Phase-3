# Core Python
import os

# Data tools
import pandas as pd

# Environment variables for API credential storage
import dotenv

# Asynchronous work
import asyncio

# GVCEH objects
import reddit_data_fetcher as rdf


if __name__ == "__main__":

    '''
    This function uses the GVCEHReddit class to retrieve 
    Reddit from specific Subreddits by searching for specific 
    keyword or search terms.
    
    See GVCEHReddit documentation for details on inputs and outputs.
    
    '''

    # import out environment variables
    dotenv.load_dotenv('../../data/environment/.env')

    # Step 1: Initialize Reddit object
    data_fetcher = rdf.GVCEHReddit(client_id=os.environ.get("REDDIT_CLIENT_ID"),
                                   client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
                                   user_agent=os.environ.get("REDDIT_USER_AGENT"))

    # Subreddits to explore
    subreddit_names = ['OakBayBritishColumbia', 'SaanichPeninsula', 'britishcolumbia',
                       'Sooke', 'Esquimalt', 'SidneyBC', 'saltspring',
                       'Metchosin', 'WestShoreBC', 'VancouverIsland', 'uvic']

    subreddit_names = ['WestShoreBC', 'VancouverIsland', 'uvic']

    # Location of post data
    posts_file_path = "../../data/reddit/posts"

    # Step 2: Fetch reddit data
    asyncio.run(data_fetcher.fetch_data(subreddit_names=subreddit_names))

    # Step 3: Aggregate data into a single dataset


