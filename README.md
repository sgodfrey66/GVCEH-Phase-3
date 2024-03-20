# SWB-GVCEH
## Project 228
### Program Sentiment Analysis - Phase III
### Update March 2024

### Summary: 
Statistics Without Borders (SWB) and the Greater Victoria Coalition to End Homelessness (GVCEH) aka The Alliance to End Homelessness in The Capital Region are jointly developing tools to monitor and assess sentiment social media posts related to GVCEH's programs. 

[The Alliance to End Homelessness in The Capital Region](https://victoriahomelessness.ca/) (AEHC, the Alliance) was formed in 2008 with a mission to end homelessness in the Capital Region. The Alliance consists of local housing, health and social service providers; non-profit organizations; all levels of government; businesses; the faith community; people with a lived experience of homelessness (past or present); and members of the general public. This diverse membership, referred to as Alliance Partners, come together to collectively address the needs of individuals experiencing homelessness in the Capital Region.

[Statistics Without Borders](https://www.statisticswithoutborders.org/) (SWB) provides pro bono statistical and data science services to nonprofit organizations and governmental agencies in research, statistical analyses, and survey design. SWB is an organization comprised entirely of volunteers and through its non-partisan and secular activities, SWB promotes the use of statistics to improve the well-being of all people.

This joint project comprising multiple phases is developing a Python pipeline to collect and assess social-media comments to better understand community sentiment towards Alliance programs.  Specifically, after Phase 3, tweets from X (formerly Twitter) and posts from Reddit are collected, scored for relevance and sentiment and then presented to users through a business intelligence dashboard.  

This repo contains code and data from Phase 3 which extends Phase 2 by adding Reddit as a data source and updating some model pipeline and modeling components.

### Logical Components:

This platform's primary functional components are to:

- Retrieve [X (Twitter)](https://twitter.com/home) tweets containing specific keywords or selected hashtags
- Retrieve [Reddit](https://www.reddit.com/) posts containing specific keywords from selected Subreddits
- Score those tweets and posts for relevance to homelessness programs in The Capital Region resulting in a binary classification of relevant or irrelevant
- Assess those tweets and posts for sentiment resulting in a multi-classification of positive, neutral or negative
- Present these findings on a dashboard

### Workflow:

This code is designed to be run either locally or on the Google Cloud Platform ([GCP](https://cloud.google.com/?hl=en)). 

To run locally (from the /code subdirectory):

    python run_scrapers.py local

To run from a GCP virtual machine (from the /code subdirectory):

    python run_scrapers.py

To run it you will need API credentials for both the X and Reddit APIs, and the follwoing environment variables will need to be set:

        # Reddit credentials
        REDDIT_CLIENT_ID
        REDDIT_CLIENT_SECRET
        REDDIT_USER_AGENT

        # XTwitter credentials
        TWITTER_BEARER_TOKEN
        TWITTER_CONSUMER_KEY
        TWITTER_CONSUMER_SECRET
        TWITTER_ACCESS_TOKEN
        TWITTER_ACCESS_TOKEN_SECRET

To set local credentials, we use [dotenv](https://pypi.org/project/python-dotenv/) and to set GCP credentials, we use [GCP Secret Manager](https://cloud.google.com/security/products/secret-manager).

The following input and output file locations need to exist and be configured for the code to function properly:

        hashtags_file_name: File containing hashtag or other key search terms
        keywords_file_name: File containing keywords

        tweets_file_path: Path to the retrieved posts or submissions
        logs_file_path: Path to the logs captured during retrieval
        keywords_file_path: Path to the CSV files with keyword search terms

        posts_file_path: Path to the retrieved posts or submissions
        logs_file_path: Path to the logs captured during retrieval

The paths configurations to run locally are the following:

        # File locations
        reddit_posts_file_path = "../data/reddit/posts"
        reddit_logs_file_path = "../data/reddit/logs"
        xtwitter_tweets_file_path = "../data/xtwitter/tweets"
        xtwitter_logs_file_path = "../data/xtwitter/logs"
        keywords_file_path = "../data/keywords"

The paths configuration we use for a GCP run are given below (note that logs are saved to the virtual machine while the keywords, Subreddits, Hashtags and scored tweet and post data are stored in GCP Cloud Storage):

        # File locations
        bucket_name = "gvceh-03a-storage"
        bucket_path = "gs://{}".format(bucket_name)
        
        reddit_posts_file_path = "{}/reddit/posts".format(bucket_path)
        reddit_logs_file_path = "{}/reddit/logs".format(bucket_path)
        reddit_logs_file_path = "../data/reddit/logs"
        
        xtwitter_tweets_file_path = "{}/xtwitter/tweets".format(bucket_path)
        xtwitter_logs_file_path = "{}/xtwitter/logs".format(bucket_path)
        xtwitter_logs_file_path = "../data/xtwitter/logs"

        keywords_file_path = "{}/keywords".format(bucket_path)

### Contents

Components in this repo include

* [code](code/) - Code used for data collection and scoring organized by data source (i.e. Reddit and X)
* [data](data/) - Data collected from production processes
* [research](research/) - Tools for researching code and data


### Contacts

- <b>Statistics Without Borders</b>
    - [Steve Godfrey](mailto:stephengodfrey223@gmail.com)
    - [Alexandra Joukova](mailto:alex.joukova@gmail.com)
    - [Danika Bellchambers](mailto:danicabellchambers@gmail.com)
    - [Akshata Upadhye](mailto:akshatarupadhye@gmail.com)
    - [Elliott Familant](mailto:efamilant@gmail.com)
  

- Alliance to End Homelessness
    - [Michelle Vanchu-Orosco](mvanchu-orosco@victoriahomelessness.ca)



