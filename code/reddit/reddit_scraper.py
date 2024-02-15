
import asyncio
import reddit_data_fetcher as rdf
import os
import dotenv


if __name__ == "__main__":

    '''
    This function uses the GVCEHReddit class to retrieve 
    Reddit from specific Subreddits by searching for specific 
    keyword or search terms.
    
    See GVCEHReddit documentation for details on inputs and outputs.
    
    '''

    # import out environment variables
    dotenv.load_dotenv('../../data/environment/.env')

    # Initialize Reddit object
    data_fetcher = rdf.GVCEHReddit(client_id=os.environ.get("REDDIT_CLIENT_ID"),
                                  client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
                                  user_agent=os.environ.get("REDDIT_USER_AGENT"))

    # subreddits = ['OakBayBritishColumbia', 'SaanichPeninsula', 'britishcolumbia',
    #               'Sooke', 'Esquimalt', 'SidneyBC', 'saltspring',
    #               'Metchosin', 'WestShoreBC', 'VancouverIsland', 'uvic']

    subreddit_names = ['britishcolumbia', 'OakBayBritishColumbia',
                       'SaanichPeninsula', 'Sooke']

    # Fetch reddit data
    asyncio.run(data_fetcher.fetch_data(subreddit_names=subreddit_names))

