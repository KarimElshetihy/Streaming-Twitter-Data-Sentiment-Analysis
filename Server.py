import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json
import sys


# Query Twitter API 
# These are hidden to comply with Twitter's API terms and conditions
consumer_key='hidden'
consumer_secret='hidden'
access_token ='hidden'
access_secret='hidden'

# auth = OAuthHandler(consumer_key, consumer_secret)
# auth.set_access_token(access_token, access_secret)
# api = tweepy.API(auth, wait_on_rate_limit=True)


# Create a StreamListener instance
class TweetsListener(StreamListener):
    # tweet object listens for the tweets
    def __init__(self, csocket):
        self.client_socket = csocket
        
    def on_data(self, data):
        try:  
            msg = json.loads( data )
            print("new message")
            # if tweet is longer than 140 characters
            
            
            if "extended_tweet" in msg:
                # add at the end of each tweet "t_end" 
                self.client_socket\
                    .send(str(msg['extended_tweet']['full_text']+"t_end")\
                    .encode('utf-8'))         
                print(msg['extended_tweet']['full_text'])

            
            else:
                # add at the end of each tweet "t_end" 
                self.client_socket\
                    .send(str(msg['text']+"t_end")\
                    .encode('utf-8'))
                print(msg['text'])
            return True
        
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
    
    def on_error(self, status):
        print(status)


# Send data from Twitter
def sendData(c_socket, keyword):
    print('start sending data from Twitter to socket')
    
    # authentication based on the credentials
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    # start sending data from the Streaming API 
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track = keyword, languages=["en"])
    

# Start Streaming
if __name__ == "__main__":
    
    # server (local machine) creates listening socket
    s = socket.socket()
    host = ''
    port = 5555
    try:
        s.bind((host, port))
    
    except socket.error as msg:
        print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()

    print('Socket bind complete')
    print('socket is ready')
    
    
    # server (local machine) listens for connections
    s.listen(4)
    print('socket is listening')
    
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    
    # select here the keyword for the tweet data
    sendData(c_socket, keyword = ['The Comeback Trail'])
    # The Comeback Trail” / “Robert De Niro”.