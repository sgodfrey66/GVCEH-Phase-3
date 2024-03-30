import os, sys

import streamlit as st

import gvceh3_utilities
from PIL import Image




###### Step 1: Set up app parameters

# set page layout
st.set_page_config(layout="wide")

# initialize streamlit containers
header = st.container()
aggregations = st.container()
sidebar = st.container()

# CSS to inject contained in a string
hide_table_row_index = """
            <style>
            tbody th {display:none}
            .blank {display:none}
            </style>
            """

# inject CSS with Markdown
st.markdown(hide_table_row_index, unsafe_allow_html=True)

# Get images
images_path = "../../data/images"
gvceh_logo = Image.open(os.path.join(images_path,
									 "branding.png"))

###### Step 2: Get data

# Instantiate a utilities object
guo = gvceh3_utilities.DashboardData()

# import data into a dictionary
# with format {"Twitter": df_x, "Reddit": df_r}
# dsource_dict = guo.dsource_dict

# import the help dictionary
readme = guo.readme

#
with header:

	a1, a2 = st.columns([2, 1])
	a1.title('Homelessness in Greater Victoria - Social Media Monitor (Phase 3)')
	a1.markdown('''This dashboard gives a sense of the sentiment around homelessness in the Greater Victoria area. 
	Data is collected from Twitter daily and a relevancy model actively filters out irrelevant tweets. Further 
	documentation and source code can be found at: https://github.com/Statisticians-Without-Borders-GVCEH/SWB-GVCEH''')
	a2.image(gvceh_logo)

	st.subheader('Twitter Activity Summary')
	st.text("{} - {}".format(guo.xtwitter_start, guo.xtwitter_end))
	kpi1, kpi2, kpi3 = st.columns(3)
	kpi1.metric(label="Total Tweets", value=f"{guo.xtwitter_tweet_cnt:,}")
	kpi2.metric(label="User Count", value=f"{guo.xtwitter_user_cnt:,}")
	kpi3.metric(label="Location Count", value=f"{guo.xtwitter_location_cnt:,}")

	st.subheader('Twitter Sentiment Summary')
	kpi1, kpi2, kpi3 = st.columns(3)
	kpi1.metric(label="Negative Rate", value=f"{guo.xtwitter_neg_rate:,.0%}")
	kpi2.metric(label="Neutral Rate", value=f"{guo.xtwitter_neu_rate:,.0%}")
	kpi3.metric(label="Positive Rate", value=f"{guo.xtwitter_pos_rate:,.0%}")

	st.subheader('Reddit Activity Summary')
	st.text("{} - {}".format(guo.reddit_start, guo.reddit_end))
	kpi1, kpi2, kpi3 = st.columns(3)
	kpi1.metric(label="Total Posts", value=f"{guo.reddit_post_cnt:,}")
	kpi2.metric(label="User Count", value=f"{guo.reddit_user_cnt:,}")
	kpi3.metric(label="Subreddit Count", value=f"{guo.reddit_subreddit_cnt:,}")

	st.subheader('Reddit Sentiment Summary')
	kpi1, kpi2, kpi3 = st.columns(3)
	kpi1.metric(label="Negative Rate", value=f"{guo.reddit_neg_rate:,.0%}")
	kpi2.metric(label="Neutral Rate", value=f"{guo.reddit_neu_rate:,.0%}")
	kpi3.metric(label="Positive Rate", value=f"{guo.reddit_pos_rate:,.0%}")

