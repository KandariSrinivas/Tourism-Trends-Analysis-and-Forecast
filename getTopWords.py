import csv
import nltk
import re
import os
from nltk.corpus import stopwords
import pandas as pd
from pyspark import SparkContext
sc = SparkContext("local", "SS")
en_stops = set(stopwords.words('english'))

nltk.download("stopwords")

def combine_csv(files, target_name):
  pd.concat([pd.read_csv(file, quoting=csv.QUOTE_NONE) for file in files]).to_csv(target_name)

def process(country_dir):
  ## country_dir will contain csvs of all tweets for that country, 
  ## note: no other csvs should be present
  ## output file is in ./ECS Data/Elastic_Data.csv
  ## returns top 1000 words
  os.chdir(country_dir)
  files = os.listdir()
  files = [file for file in files if '.csv' in file]
  # return files
  for file in files:  
    RDD = sc.textFile(country_dir + "/" +file) \
        .map(lambda line: line.split(",")) \
        .filter(lambda line: len(line)>1) \
        .map(lambda line: (line[1].split(" ")[0] +","+ file.split('_')[0]  + "," + line[7])) \
        .saveAsTextFile(country_dir + "/ECS Data/" +"EC_input "+ file.split("_")[-1].split(".csv")[0] +".csv")
  
  os.chdir(country_dir + "/ECS Data")
  result_csvs = [file + "/" + "part-00000" for file in os.listdir() if ".csv" in file]
  combine_csv(result_csvs, "Elastic_Data.csv")

  top_words = []

  file = "Elastic_Data.csv"
  RDD = sc.textFile(country_dir +"/ECS Data"+ "/" +file) \
      .map(lambda line: line.split(",")) \
      .filter(lambda line: len(line)>1) \
      .map(lambda line: re.sub(r'http\S+', '', line[3]))  \
      .flatMap(lambda line: re.findall(r"\s*[a-zA-Z]+", line)) \
      .map(lambda word: (word.lower().strip(), 1)) \
      .filter(lambda word: word[0] not in en_stops and len(word[0])>1) \
      .reduceByKey(lambda a,b: a+b) \
      .sortBy(lambda row: row[1], ascending=False) \
      # .map(lambda row: row[0])

  top_words.append(RDD.take(1000))
  return top_words

if __name__ == "__main__":
  top_words = process(os.getcwd() +"/Singapore")    