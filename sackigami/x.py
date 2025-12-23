import requests
import tweepy

from sackigami.constants import API_CRED


def connect_to_client() -> tweepy.Client:
    """Connect to the tweepy/X client.

    Returns:
        tweepy.Client: The connected client.
    """
    return tweepy.Client(
        consumer_key=API_CRED.api_key,
        consumer_secret=API_CRED.api_secret,
        access_token=API_CRED.access_token,
        access_token_secret=API_CRED.access_secret,
    )


def post(text: str) -> requests.Response:
    """Post a post on X.

    Args:
        text (str): Text to post.

    Returns:
        requests.Response: Response by the client.
    """
    client: tweepy.Client = connect_to_client()
    response: requests.Response = client.create_tweet(text=text)
    return response
