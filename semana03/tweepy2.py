#imports
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import StreamListener
import socket
import json
from decouple import config

#Set up the credencials
consumer_key = 'consumer_key'
consumer_secret = 'consumer_secret'
access_token = 'access_token'
access_secret = 'access_secret'

class TweetsListener(StreamListener):
    """Standard features"""
    def __init__(self, csocket):
        self.client_socket = csocket
        self.count = 0
        self.limit = 30


    def on_data(self, data):
        """reads and stores the tweets, checks the amount read and sends the message to the socket"""
        try:
            msg = json.loads(data)
            self.count += 1
            if self.count <= self.limit:
                print(msg['text'].encode('utf-8'))
                self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        """Check for error messages"""
        print(status)
        return True

    def sendData(c_socket):
        """Authentication Twitter API and sending data"""

        #Authentication
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token,access_secret)

        twitter_stream = Stream(auth, TweetsListener(c_socket)) #define the type of conection
        twitter_stream.filter(track=['Bolsonaro'])

    if __name__ == '__main__':
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #Local socket (IPv6, ICP)
        #local config
        host = '127.0.0.1'
        port = 9999
        s.bind((host, port))

        print("Listening on port %s " % str(port))

        s.listen(5) #wait for connection
        c, addr = s.accept()

        print("Received request from: " + str(addr))