import sys
import json
import re
import numpy as np
from scipy import stats

from pyspark import SparkContext
from getTopWords import process

def addWordsToBV():
    top_words = process(os.getcwd() + country_name)
    return res

def setInput(row):
    inp = row
    try:
        tweet = str(inp['tweet'])
        country = str(inp['country'])
        datee = inp['date'].replace('T', ' ')
        orgDate = datetime.datetime.strptime(datee, '%Y-%m-%d %H:%M:%S')
        date = str(orgDate.date())
        
    except KeyError:
        tweet = ""
        date = ""
        country = ""

    return ((date,country),(tweet))


def calculateRelFreq(row):
    date = row[0][0]
    country = row[0][1]
    vals = row[1]
    total_words = 0
    ff = getTrendScoreFootfall(date,country) 
    footfall = ff[1]
    res = [0]*len(bvCommonWords.value)
    for t in vals:
        words = re.findall(r'((?:[\.,!?;"])|(?:(?:\#|\@)?[A-Za-z0-9_\-]+(?:\'[a-z]{1,3})?))', t.lower())
        total_words += len(words)
        
        for i in range(0,len(bvCommonWords.value)):
            res[i] += words.count(bvCommonWords.value[i])
    output = []
    for i in range(0,len(res)):
        res[i] = res[i]/total_words
        output.append(((bvCommonWords.value[i],country),(res[i],footfall)))
    
    return output

def getAllRelFreq(row):
    word = row[0][0]
    country = row[0][1]
    values = row[1]
    relFreq = []
    footfall = []
    for v in values:
        relFreq.append(v[0])
        footfall.append(v[1])
    return ((word,country),(relFreq,footfall))

def calculateBeta(row):
    word = row[0][0]
    country = row[0][1]
    x = np.array(row[1][0])
    y = np.array(row[1][1])
    
    x = (x-np.mean(x))/np.std(x)
    y = (y-np.mean(y))/np.std(y)

    allOnes = [1]*len(row[1][0])
    
    X = np.column_stack((allOnes, x))
    
    BetaCap = np.dot(X.transpose(),X)
    BetaCap = np.linalg.pinv(BetaCap)
    BetaCap = np.dot(BetaCap,X.transpose())
    BetaCap = np.dot(BetaCap,y)
    
    beta0 = float(BetaCap[0])
    beta1 = float(BetaCap[1])
    
    rss = pow(y - beta0 - beta1*x,2)
    df = len(row[1][1])-2
        
    s_2 = rss.sum()/df
    t_val_deno = len(row[1][1])
    t_val = beta1/pow(s_2/t_val_deno,0.5)
    p_val = stats.t.sf(np.abs(t_val), df)*2
    
    return (country,(word,float(beta1),p_val*1000))


def helper(x):
    output = []
    vals = x[1]
    country = x[0]
    arr = []
    for i in vals:
        arr.append(i)
    arr.sort(key = lambda x: -x[1])
    
    output.append(("Top 25 positive words",(arr[:25])))
    last25 = arr[-25:]
    last25.reverse()
    output.append(("Top 25 negative words",(last25)))
    return output


def getCount(row):
    date = row[0][0]
    country = row[0][1]
    vals = row[1]
    total_words = 0
    ff = getTrendScoreFootfall(date,country) 
    
    res = {}
    for i in bvCommonWords.value:
        res[i]=0

    for t in vals:
        words = re.findall(r'((?:[\.,!?;"])|(?:(?:\#|\@)?[A-Za-z0-9_\-]+(?:\'[a-z]{1,3})?))', t.lower())
        for i in res.keys():
            res[i] += words.count(i)
    
    return {"date":date,"country":country,"footfall":ff[1],"trend_score":ff[0],"words":res}


if __name__ == "__main__":
    cloudID = ""
    username = ""
    password = ""
    elastic = ELK(cloudID, username, password)
   
    
    sc = SparkContext.getOrCreate()
    
    inp = sc.parallelize(elastic.getData("tweets",""))
    
    #Add common words to broadcast variable 
    bvCommonWords = sc.broadcast(addWordsToBV())
    
    inpData = inp.map(lambda x: setInput(x)) \
                      .groupByKey()

    # #Calculate relative freq for each word
    wordsRelFreq = inpData.flatMap(lambda x: calculateRelFreq(x)) \
                          .groupByKey() \
                          .map(lambda x:getAllRelFreq(x))
    
    # #Calculate beta, pval 
    allBeta = wordsRelFreq.map(lambda x: calculateBeta(x)).groupByKey().flatMap(lambda x: helper(x)).collect()

    topPosNegWords = []
    
    #Print results for top positive and negative words and save this data to Elasticsearch
    for row in allBeta:
        print(row[0])
        print('\n')
        for word in row[1]:
            elastic.indexData("hypothesis",float(word[2]))
            topPosNegWords.append(word[0])
        print('\n')
        print('\n-------------------XXX------------------\n')

    

    #Process and save data to elasticsearch for forecasting
    bvCorrWords = sc.broadcast(topPosNegWords)
    
    procOutput = inpData.map(lambda x: getCount(x)).collect()

    for r in procOutput:
        elastic.indexData("processed_data",r)
