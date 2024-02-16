# SWB-GVCEH
## Project 228
### Program Sentiment Analysis - Phase III

### Summary: 
Statistics Without Borders (SWB) and the Greater Victoria Coalition to End Homelessness (GVCEH) aka The Alliance to End Homelessness in The Capital Region are jointly developing tools to monitor and assess sentiment social media posts related to GVCEH's programs. 

[The Alliance to End Homelessness in The Capital Region](https://victoriahomelessness.ca/) (AEHC, the Alliance) was formed in 2008 with a mission to end homelessness in the Capital Region. The Alliance consists of local housing, health and social service providers; non-profit organizations; all levels of government; businesses; the faith community; people with a lived experience of homelessness (past or present); and members of the general public. This diverse membership, referred to as Alliance Partners, come together to collectively address the needs of individuals experiencing homelessness in the Capital Region.

[Statistics Without Borders](https://www.statisticswithoutborders.org/) (SWB) provides pro bono statistical and data science services to nonprofit organizations and governmental agencies in research, statistical analyses, and survey design. SWB is an organization comprised entirely of volunteers and through its non-partisan and secular activities, SWB promotes the use of statistics to improve the well-being of all people.

This joint project comprising multiple phases is developing a Python pipeline to collect and assess social-media comments to better understand community sentiment towards Alliance programs.  Specifically, after Phase 3, tweets from X (formerly Twitter) and posts from Reddit are collected, scored for relevance and sentiment and then presented to users through a business intelligence dashboard.  

This repo contains code and data from Phase 3 which extends Phase 2 by adding Reddit as a data source and updating some model pipeline and modeling components.

### Contents

Components in this repo include

* [code](code/) - Code used for data collection and scoring organized by data source (i.e. Reddit and X)
* [data](data/) - Data collected from production processes
* [research](research/) - Tools for researching code and data
