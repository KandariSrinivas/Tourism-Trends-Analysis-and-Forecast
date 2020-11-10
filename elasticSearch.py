from elasticsearch import Elasticsearch
from mappings import *

class ELK:
	def __init__(self,cloudID,username,password):
		self.es = Elasticsearch(
    		cloud_id=cloudID,
    		http_auth=(username,password),
		)


	def createIndex(self,indexName,mappings):
		print("Creating index for index = %s" % indexName)
		if self.es.indices.exists(indexName):
			self.deleteIndex(indexName)
		print(self.es.indices.create(index=indexName, ignore=400, body=mappings))



	def indexData(self,indexName,data):
		print("Indexing data to index = %s, data = %s" % (indexName, data))
		res = self.es.index(index=indexName,body=data)
		print(res)


	def getData(self,indexName,query):
		print("Searching data for index = %s" % indexName)
		allData = []
		if query!='':
			res = self.es.search(index=indexName,body=query)
		else:
			res = self.es.search(index=indexName)
		allRows = res['hits']['hits']
		for row in allRows:
			allData.append(row['_source'])
		return allData

	def deleteIndex(self,indexName):
		print("Deleting index = %s" % indexName)
		print(self.es.indices.delete(index=indexName, ignore=[400, 404]))

if __name__ == "__main__":
	cloudID = "bda:dXMtZWFzdDEuZ2NwLmVsYXN0aWMtY2xvdWQuY29tOjkyNDMkNDQwZjI0MDg3YTk1NGRmMGJhMmUyNmFjMmYxMmVjYWUkM2RmYTc4NDg2YzczNGVmM2I0ZTFhNTBhZTYwYWQ2YTM="
	username = "elastic"
	password = "QtXdCDEPbjSuhzwWN1Ss33tL"
	
	elastic = ELK(cloudID, username, password)
	
	#Create index
	elastic.createIndex("tweet",tweet_mappings)
	
	data = [{"date":"2019-06-29 23:00:51","country":"Singapore","tweet":"Marina Bay"},
			{"date":"2019-06-29 23:00:51","country":"Singapore","tweet":"Gardens by the Bay"},
			{"date":"2019-06-29 23:00:51","country":"Singapore","tweet":"Singapore Zoo"},
			{"date":"2019-06-29 23:00:51","country":"Singapore","tweet":"Night Safari"},
			{"date":"2019-06-29 23:00:51","country":"Singapore","tweet":"Hotel Raffles"},
			{"date":"2019-06-29 23:00:51","country":"Singapore","tweet":"Sentosa"},
			{"date":"2019-06-29 23:00:51","country":"Singapore","tweet":"Universal Studios"},
			{"date":"2019-06-29 23:00:51","country":"Singapore","tweet":"SG"},
			{"date":"2019-06-29 23:00:51","country":"Singapore","tweet":"Singapore"},
			{"date":"2019-06-29 23:00:51","country":"Singapore","tweet":"Changi"},
			{"date":"2019-06-29 23:00:51","country":"Singapore","tweet":"F1 SG"}]

	for d in data:
		elastic.indexData("tweet",d)





	
	


