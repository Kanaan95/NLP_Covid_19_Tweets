from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream, API
import creds
import numpy as np
import pandas as pd
import json
import datetime

class TwitterAuthenticator():
    def __init__(self):
        self.auth = OAuthHandler(creds.API_KEY, creds.API_SECRET)
        self.auth.set_access_token(creds.ACCESS_TOKEN, creds.ACCESS_TOKEN_SECRET)

    def get_auth(self):
        return self.auth

"""
TWITTER CLIENT CLASS
"""
class TwitterClient():
    def __init__(self, twitter_user = None):
        self.auth = TwitterAuthenticator().get_auth()
        self.twitter_client = API(self.auth, wait_on_rate_limit=True)

    def tweets_30_days(self, devName, query):
        q = ' OR '.join(query)
        return self.twitter_client.search_30_day(devName, q, maxResults = 100)

class TwitterStream():
    """
    Class for streaming and processing live tweets
    """
    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, detailed_tweets, hash_tag_list, df, csv_path):
        '''
        Creates and launch the stream to capture live tweets and store them

            Parameters:
                    fetched_tweets_filename (string): path to store general info tweets
                    detailed_tweets (string): path to store detailed tweets
                    hash_tag_list (list): list of hashtags to search
                    df (Pandas Dataframe): dataframe to store the general info tweets
                    csv_path: path to store the csv
        '''
        listener = StdOutListener(fetched_tweets_filename, detailed_tweets, df, csv_path)
        auth = TwitterAuthenticator().get_auth()
        stream = Stream(auth, listener)

        stream.filter(languages=["en"], is_async = True, track=hash_tag_list)


class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, fetched_tweets_filename, detailed_tweets, df, path):
        super(StdOutListener, self).__init__()
        self.fetched_tweets_filename = fetched_tweets_filename
        self.detailed_tweets = detailed_tweets
        self.dataframe = df
        self.path = path

    def on_status(self, status):

        # Store Full tweet
        with open(self.detailed_tweets, 'a', encoding="utf-8") as tf1:
                tf1.write(json.dumps(status._json))
                tf1.write("\n,")
                tf1.write("\n")

        tweet = {
            "id": status.id,
            "user_name": status.user.screen_name,
            "user_location": status.user.location,
            "user_description": status.user.description,
            "user_created": status.user.created_at,
            "user_followers": status.user.followers_count, 
            "user_friends": status.user.friends_count,
            "user_favorites": status.user.favourites_count,
            "user_verified": status.user.verified,
            "date": status.created_at,
            "source": status.source,
            "is_retweet": status.retweeted
        }

        if hasattr(status, "retweeted_status"):  # Check if Retweet
            try:
                tweet["text"] = status.retweeted_status.extended_tweet["full_text"]
                tweet["hashtags"] = status.retweeted_status.extended_tweet.entities["hashtags"]
            except AttributeError:
                tweet["text"] = status.retweeted_status.text
                tweet["hashtags"] = status.retweeted_status.entities["hashtags"]
        else:
            try:
                tweet["text"] = status.extended_tweet["full_text"]
                tweet["hashtags"] = status.extended_tweet.entities["hashtags"]
            except AttributeError:
                tweet["text"] = status.text
                tweet["hashtags"] = status.entities["hashtags"]

        with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(json.dumps(tweet, sort_keys= True, default = lambda o : o.__str__() if isinstance(o, datetime.datetime) else o))
                tf.write(",")
                tf.write("\n")

        self.dataframe = self.dataframe.append(tweet, ignore_index = True)
        self.dataframe.to_csv(self.path, encoding="utf-8", index=False)

        return True

    def on_error(self, status):
        if status == 420:
            # Returning False on_data method in case rate limit occurs.
            return False
        print(status)


