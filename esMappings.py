#Mappings for indexeas
tweet_mappings = {
            "mappings": {
                "properties": {
                    "tweet": {
                        "type": "text"
                    },
                    "country": {
                        "type": "keyword"
                    },
                    "date": {
                        "type" : "date"
                    }
                }
            }
        }


trends_mappings = {
            "mappings": {
                "properties": {
                    "trend_score": {
                        "type": "integer"
                    },
                    "country": {
                        "type": "keyword"
                    },
                    "date": {
                        "type" : "date"
                    }
                }
            }
        }


hypothesis_output_mappings = {
            "mappings": {
                "properties": {
                    "score": {
                        "type": "float"
                    },
                    "country": {
                        "type": "keyword"
                    },
                    "date": {
                        "type" : "date"
                    },
                    "word": {
                        "type" : "keyword"
                    }
                }
            }
        }


processed_data_mappings = {
            "mappings": {
                "properties": {
                    "trend_score": {
                        "type": "float"
                    },
                    "footfall": {
                        "type": "long"
                    },
                    "country": {
                        "type": "keyword"
                    },
                    "date": {
                        "type" : "date"
                    },
                    "words": {
                        "type" : "object"
                    }
                }
            }
        }