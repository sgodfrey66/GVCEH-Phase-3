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
images_path = os.path.dirname(__file__)
# my_file = path+'/photo.png'

# images_path = "images"
# gvceh_logo = Image.open(os.path.join(images_path,
# 									 "branding.png"))
gvceh_logo = Image.open(images_path+"branding.png")

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
	h_msg = ("This dashboard gives a sense of the sentiment around homelessness and homelessness programs "
			 "in the Greater Victoria British Columbia area of Canada. "
			 "Data is periodically collected from X (Twitter) and Reddit and scored for relevancy and sentiment "
			 "using various natural language processing models. "
			 "The work is a joint effort of the [Alliance to End Homelessness in the Capital Region](https://victoriahomelessness.ca/) "
			 "and [Statistics Without Borders](https://www.statisticswithoutborders.org/), and "
			 "further documentation and source code can be found at [GVCEH-Phase-3](https://github.com/sgodfrey66/GVCEH-Phase-3).")
	a1.write(h_msg)
	a2.image(gvceh_logo)

	st.markdown("""---""")
	st.write("# Activity Statistics")
	st.write("## Twitter")
	st.write("#### Activity ({} - {})".format(guo.xtwitter_start, guo.xtwitter_end))

	kpi1, kpi2, kpi3, kpi4 = st.columns(4)
	kpi1.metric(label="Reviewed Tweets", value=f"{guo.xtwitter_tweet_cnt:,}")
	kpi2.metric(label="Relevant Tweets", value=f"{guo.xtwitter_rel_tweet_cnt:,}")
	kpi3.metric(label="User Count", value=f"{guo.xtwitter_user_cnt:,}")
	kpi4.metric(label="Location Count", value=f"{guo.xtwitter_location_cnt:,}")

	st.write("#### Sentiment ")
	kpi1, kpi2, kpi3 = st.columns(3)
	kpi1.metric(label="Negative Rate", value=f"{guo.xtwitter_neg_rate:,.0%}")
	kpi2.metric(label="Neutral Rate", value=f"{guo.xtwitter_neu_rate:,.0%}")
	kpi3.metric(label="Positive Rate", value=f"{guo.xtwitter_pos_rate:,.0%}")

	st.write("## Reddit")
	st.write("#### Activity ({} - {})".format(guo.reddit_start, guo.reddit_end))

	kpi1, kpi2, kpi3, kpi4 = st.columns(4)
	kpi1.metric(label="Reviewed Posts", value=f"{guo.reddit_post_cnt:,}")
	kpi2.metric(label="Relevant Posts", value=f"{guo.reddit_rel_post_cnt:,}")
	kpi3.metric(label="User Count", value=f"{guo.reddit_user_cnt:,}")
	kpi4.metric(label="Subreddit Count", value=f"{guo.reddit_subreddit_cnt:,}")

	st.write("#### Sentiment ")
	kpi1, kpi2, kpi3 = st.columns(3)
	kpi1.metric(label="Negative Rate", value=f"{guo.reddit_neg_rate:,.0%}")
	kpi2.metric(label="Neutral Rate", value=f"{guo.reddit_neu_rate:,.0%}")
	kpi3.metric(label="Positive Rate", value=f"{guo.reddit_pos_rate:,.0%}")

	# Data downloads
	st.markdown("""---""")
	st.write("# Data Downloads")

	dld1, dld2 = st.columns(2)
	@st.cache_data
	def convert_df(df):
	   return df.to_csv(index=False).encode('utf-8')

	# Filter for relevant data
	maskr = guo.df_r["is_relevant"] == 1
	df_r_dld = convert_df(guo.df_r[maskr])
	dld1.download_button(label="Reddit Data Download (Relevant Posts)",
					   data=df_r_dld,
					   file_name="reddit_posts_scored.csv",
					   mime="text/csv",
					   key='download-r-csv')

	# Filter for relevant data
	maskx = guo.df_x["is_relevant"] == 1
	df_x_dld = convert_df(guo.df_x[maskx])
	dld2.download_button(label="X (Twitter) Data Download (Relevant Tweets)",
					   data=df_r_dld,
					   file_name="xtwitter_tweets_scored.csv",
					   mime="text/csv",
					   key='download-x-csv')