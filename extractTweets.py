# ExtractTweets.py
# This program is to extract tweets on a regular basis to collect dataset for CSE545: Final Project
# The program is executed daily by changing input dictionary to collect tweets for criterion relevant to our project.
#
import GetOldTweets3 as got
import csv
import datetime as dt
#
# Input to extract Tweets
dict_tspots = {'Singapore':['Marina Bay', 'Changi', 'Sentosa', 'Gardens by the Bay',
                            'Botanical Garden Singapore', 'Singapore Zoo', 'Universal Studios Singapore',
                            'Clarke Quay', 'Night Safari Singapore', 'Merlion']}
since_dateStr = '2019-01-01'
until_dateStr = '2020-04-30'
language = 'en'
# Starting to extract tweets based on above inputs
#
since_date = dt.datetime.strptime(since_dateStr, '%Y-%m-%d').date()
until_date = dt.datetime.strptime(until_dateStr, '%Y-%m-%d').date()
#
for key in dict_tspots.keys():
    for value in dict_tspots[key]:
        since = since_date
        until = since + dt.timedelta(days=7)           # Extracting tweets for one week at a time to avoid hitting the limit.
        outFile = key + '_' + value + '.csv'
        csvFile = open(outFile, 'w')
        csvWriter = csv.writer(csvFile)
        while (until <= until_date):
            tweetCriteria = got.manager.TweetCriteria().setQuerySearch(value).setSince(str(since)).setUntil(str(until))
            tweets = got.manager.TweetManager.getTweets(tweetCriteria)
            for tweet in tweets:
                csvWriter.writerow([tweet.date, tweet.id, tweet.username, tweet.text])
            since = until + dt.timedelta(days=1)
            until = since + dt.timedelta(days=7)
        csvFile.close()