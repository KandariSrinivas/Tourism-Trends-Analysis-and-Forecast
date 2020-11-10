# SparkReceiver.py
# This is the receiver which will receive tweets from TwitterStreaming.py
# The output is saved in a single file called part-00000 in the latest created folder at location /tweets/
#
import time
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
#
sc = SparkContext("local[2]", "Twitter Streamer")
ssc = StreamingContext(sc, 10)
host = "localhost"
port = 4040
lines = ssc.socketTextStream(host, port)
#
lines.foreachRDD(lambda stream: stream.coalesce(1).saveAsTextFile("./tweets/%f" % time.time()))
#
# Streaming is started here and will continue until stopped thru Keyboard Interrupt
ssc.start()
ssc.awaitTermination()
# ssc.stop(stopSparkContext=False, stopGraceFully=True)