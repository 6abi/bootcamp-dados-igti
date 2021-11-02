#imports
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import socket
import json
from decouple import config

#Set up the credencials
consumer_key = config("consumer_key")
comsumer_secret = config("comsumer_secret")
access_token = config("access_token")
access_secret = config("access_secret")

class  MyStreamListener(tweepy.StreamListener):
    def __init__(self):
        pass