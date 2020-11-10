CSE 545 : Final Project
Topic: Tourist Footfall prediction using Social Media Analysis

Group members:
Rohit Chaudhary (112687906)
Dheeraj Ramchandani(112970301)
Prayag Goyal (112961923)
Srinivas Kandari (112713946)

Two Data Pipelines:

Spark streaming:
It gets input from twitter as a stream using tweepy. Each batch is then pre processed and hypothesis testing and forecasting is done

Analytics (Elasticsearch):
Elasticsearch and kibana are used for real time data analytics. After every stage (pre process, hypothesis testing, forecasting) the data is indexed in Elasticsearch.
Visualizations are created on Kibana.

Two Concepts:

Hypothesis:
Hypothesis testing is done to find top ‘n’ positively and negatively correlated words related to a country’s tourist attractions with a particular date and tourist footfall in that country.

Regression/Forecasting:
We created a two layer Neural Network using Tensorflow and used the data containing date, footfall, trend_score and output words from the Hypothesis phase. The network learns on this data and it will be used to forecast footfall for future days.

Code running System:
Elasticsearch and Kibana running as separate Elastic cloud service on Google Cloud
Spark streaming is deployed on Dataproc cluster in GCP





Step6. FileName and Description:

getTrendScoreFootfall.py
Returns the Google trend score and tourist footfall for a particular country at a particular date.

extractTweets.py
Extract predated tweets within a given date range and push to Spark streaming.

twitterStreaming.py
Receives streaming data from Twitter based on the given keywords and sends the entire tweet to Spark for further processing.

sparkReceiver.py
This is a receiver program for incoming Twitter streams. The program receives the stream and preprocesses the data before sending it further for hypothesis testing and forecasting.

elasticSearch.py
Elasticsearch and kibana are used for real time data analytics. After every stage (pre process, hypothesis testing, forecasting) the data is indexed in Elasticsearch.

forecast.py
Read elasticsearch data to train the neural network and predict the footfall for future days.

getTopWords.py
Return top 1000 words related to a country in the ‘Travel’ search.

hypothesis.py
Calculating top 25 positive and top 25 negative correlated words among 1000 most commonly used words from tweets related to country and date.

esMappings.py
Contains mappings for elasticsearch indexes



General Description:

Step1: Accessing Tweets:
Method1: 
We have downloaded tweets on a per day basis using a python script which are related to tourist attractions of a country.
The tweets have downloaded in CSV files based on the search criteria specified in the script to generate the initial hypotheses.

Method2: Spark Streaming:
This program is to extract tweets on a regular basis to collect live tweets.
The program 'ExtractTweets.py' is executed daily by changing the input dictionary to collect tweets for criteria relevant to our project.

All tweets have been processed through Spark streaming. After processing, the tweets are in the form {'Date, Country, Tweet Text'}. 
The processed data goes to Elasticsearch.
Each Tweet text is split into words. The relative frequency of each word in the tweet text is updated in the matrix.


Step2: Accessing Google Trends:

The tweet files are indexed with a particular date and country as key. The script 'getRankFootfall.py' retrieves the word rank in trending index from Google trends on a scale of 0 to 100 for the selected word for that date and country, and also the tourist footfall of the selected country on that particular date.

Input would be {'Date’, ‘Country'}
	The output of above function is {Normalised Ranking on Google Trend, Footfall}

The final dictionary containing {‘Date’, ‘Country’, ‘TrendScore’, ‘Footfall’, List of words with frequency count for that date and country{}}' is also written to Elasticsearch.


Step3: Hypotheses Testing:

We have now performed hypothesis testing post data cleaning. For data cleaning, we have removed stop words and special characters from tweet texts. 
Next step is to calculate the count of each of 1000 words per date and country with footfall. Top 25 and lowest 25 correlated words with the footfall is the output of the hypothesis testing.


Step4: Prediction using Tensorflow: 

We query data from Elastic search for a specific country and use that data to train a two layer Neural Network. We use this model to predict the footfall for future dates
We also query streaming data from Elasticsearch. And use this new data to retrain the already trained model.
We also have utility functions; save_model to save the model to disk and load_model to load the model from disk

The results of both step 3 and step 4 are again stored in the Elasticsearch. 


Step5: Visualisation:
The data is read from the Elasticsearch and output corresponding visualisations are generated. Visualizations are created on Kibana.
