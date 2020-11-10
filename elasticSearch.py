from elasticsearch import Elasticsearch
from es_mappings import *

# ELK class is for elasticsearch 
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
			res = self.es.search(index=indexName,body=query,size=10000)
		else:
			res = self.es.search(index=indexName,size=10000)
		allRows = res['hits']['hits']
		for row in allRows:
			allData.append(row['_source'])
		return allData

	def getScrollData(self,indexName,query):
		print("Searching data for index = %s" % indexName)
		allData = []
		res = self.es.search(index=indexName,body=query,scroll = '2s')
       	prevScrollID = res['_scroll_id']
        while len(res['hits']['hits']):
            res = self.es.scroll(scroll_id=prevScrollID,scroll = '2s')
            prevScrollID = resp['_scroll_id']
            allRows = res['hits']['hits']
            for row in allRows:
				allData.append(row['_source'])
		return allData

	def deleteIndex(self,indexName):
		print("Deleting index = %s" % indexName)
		print(self.es.indices.delete(index=indexName, ignore=[400, 404]))

if __name__ == "__main__":
	cloudID = ""
	username = ""
	password = ""
	
	elastic = ELK(cloudID, username, password)

	
	#Create index
	elastic.createIndex("tweet",tweet_mappings)
	elastic.createIndex("trends",trends_mappings)
	elastic.createIndex("hypothesis",hypothesis_output_mappings)
	elastic.createIndex("processed_data",processed_data_mappings)






	
	


