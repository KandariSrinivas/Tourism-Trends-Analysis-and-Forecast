import sys
import json
import re
import numpy as np
from scipy import stats

from pyspark import SparkContext

def addWordsToBV():
    res = ['universal','singapore']
    return res

def setInput(row):
    inp = row
    try:
        tweet = str(inp['tweet'])
        country = str(inp['country'])
        orgDate = datetime.datetime.strptime(inp['date'], '%Y-%m-%d %H:%M:%S')
        date = str(orgDate.date())
        x = getTrendScoreFootfall(date,country) 
        footfall = x[1]
    except KeyError:
        tweet = ""
        footfall = 0 
        date = ""
        country = ""

    return ((date,country),(tweet,footfall))


def calculateRelFreq(row):
    date = row[0][0]
    country = row[0][1]
    vals = row[1]
    total_words = 0
    footfall = 0 
    res = [0]*len(bvCommonWords.value)
    for t in vals:
        footfall = t[1]
        words = re.findall(r'((?:[\.,!?;"])|(?:(?:\#|\@)?[A-Za-z0-9_\-]+(?:\'[a-z]{1,3})?))', t[0].lower())
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
    
    output.append(("Top 20 positive words",(arr[:20])))
    last20 = arr[-20:]
    last20.reverse()
    output.append(("Top 20 negative words",(last20)))
    return output

if __name__ == "__main__":
    cloudID = "bda:dXMtZWFzdDEuZ2NwLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkNDQwZjI0MDg3YTk1NGRmMGJhMmUyNmFjMmYxMmVjYWUkM2RmYTc4NDg2YzczNGVmM2I0ZTFhNTBhZTYwYWQ2YTM="
    username = "elastic"
    password = "QtXdCDEPbjSuhzwWN1Ss33tL"
    elastic = ELK(cloudID, username, password)
   
    #Create index
    elastic.createIndex("tweets",tweet_mappings)
    #elastic.createIndex("hypothesis_output",hypothesis_output_mappings)
    
    sc = SparkContext.getOrCreate()
    #inp = sc.textFile('/content/drive/My Drive/BDAProject/tweets_singapore.csv')
    inp = sc.parallelize(elastic.getData("tweet",""))
    
    #Add common words to broadcast variable 
    bvCommonWords = sc.broadcast(addWordsToBV())
    
    #Calculate relative freq for each word
    wordsRelFreq = inp.map(lambda x: setInput(x)) \
                      .filter(lambda x: x[1][1]>=0) \
                      .groupByKey() \
                      .flatMap(lambda x: calculateRelFreq(x)) \
                      .groupByKey() \
                      .map(lambda x:getAllRelFreq(x))
    
    #Calculate beta, pval 
    allBeta = wordsRelFreq.map(lambda x: calculateBeta(x)).groupByKey().flatMap(lambda x: helper(x)).collect()

    #Print results
    for row in allBeta:
        print(row[0])
        print('\n')
        for word in row[1]:
            print(word)
        print('\n')
        print('\n-------------------XXX------------------\n')
    
    sc.stop()