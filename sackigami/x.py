import os

import tweepy
from dotenv import load_dotenv
from tweepy.pagination import Response

load_dotenv()


API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_SECRET = os.getenv("ACCESS_SECRET")

client = tweepy.Client(
    consumer_key=API_KEY,
    consumer_secret=API_SECRET,
    access_token=ACCESS_TOKEN,
    access_token_secret=ACCESS_SECRET,
)


response = client.create_tweet(text="Hello World!")

print(f"{type(Response)=}")
print("Tweet sent", response)
