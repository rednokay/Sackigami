import os
import tweepy
import requests
from dotenv import load_dotenv
from dataclasses import dataclass

load_dotenv()


@dataclass(frozen=True)
class APICred:
    api_key = os.getenv("API_KEY")
    """Public X API key"""

    api_secret = os.getenv("API_SECRET")
    """Secret X API key"""

    access_token = os.getenv("ACCESS_TOKEN")
    """Public X API access token"""

    access_secret = os.getenv("ACCESS_SECRET")
    """Secret X API acccess token"""


cred: APICred = APICred()


def connect_to_client() -> tweepy.Client:
    """Connect to the tweepy/X client.

    Returns:
        tweepy.Client: The connected client.
    """
    return tweepy.Client(
        consumer_key=cred.api_key,
        consumer_secret=cred.api_secret,
        access_token=cred.access_token,
        access_token_secret=cred.access_secret,
    )


def post(text: str) -> requests.Response:
    """Post a post on X.

    Args:
        text (str): Text to post.

    Returns:
        requests.Response: Response by the client.
    """
    client: tweepy.Client = connect_to_client()
    response = client.create_tweet(text=text)
    return response
