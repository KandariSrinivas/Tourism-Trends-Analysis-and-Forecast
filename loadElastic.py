from elasticsearch import Elasticsearch
from elasticSearch import ELK

cloudID = ""
username = ""
password = ""
elastic = ELK(cloudID, username, password)

def format_date(date):
    return "T".join(date.split(" "))

def exportData(country_dir):
    for line in open(country_dir +"/ECS Data"+ "/" +'Elastic_Data.csv'):
        if 'date' not in line[1]:
            if count<5:
                print(line, count)
            line = line.split(',')
            dic = {"date": format_date(line[1]), "country": line[2], "tweet": line[3] }
            elastic.indexData("tweets", dic)
            count +=1