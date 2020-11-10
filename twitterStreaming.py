# TwitterStreaming.py
# This first python script doesn’t use Spark at all:
# This will receive streaming data from Twitter based 
# on the keywords given and extracts date and tweet text from incoming stream
# and send it to Spark for further processing
#
import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import API
import socket
import json

# Keys to access tweepy API 
consumer_key = os.environ['TWITTER_CONSUMER_KEY']
consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
access_token_key = os.environ['TWITTER_ACCESS_TOKEN']
access_token_secret = os.environ['TWITTER_ACCESS_SECRET']
#
keywords = ['Marina Bay', 'Changi', 'Sentosa', 'Gardens by the Bay',
            'Botanical Garden Singapore', 'Singapore Zoo', 'Universal Studios Singapore',
            'Clarke Quay', 'Night Safari Singapore', 'Merlion']
#languages=['en']
# 
class TweetsStreamer(StreamListener):
 
    def __init__(self, csocket):
        self.client_socket = csocket
 
    def on_data(self, data):
        try:
            print(data.split('\n'))
            self.client_socket.send(bytes(data, 'utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True
 
def sendData(connSocket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token_key, access_token_secret)
    api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    twitterStream = Stream(api.auth, TweetsStreamer(connSocket), tweet_mode = 'extended')
    try:
        twitterStream.filter(track=keywords)
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        print('Done.')
    twitterStream.disconnect()
#
if __name__ == "__main__":
    sock = socket.socket()     # Create a socket object
    host = "localhost"      # Get local machine name
    port = 4040             # Reserve a port for your service.
    sock.bind((host, port))    # Bind to the port
 
    print("Listening on port: %s" % str(port))
 
    sock.listen(5)                 # Now wait for client connection.
    conn, addr = sock.accept()        # Establish connection with client.
 
    print("Received request from: " + str(addr))
 
    sendData(conn)