from Twitter import *
import pandas as pd
import tweepy
import os

if __name__ == '__main__':

    # Path to save the CSV file
    path = "./Tweepy/tweets.csv"

    # We set the columns name to extract them from the live tweets
    cols = [
        "id",
        "user_name",
        "user_location",
        "user_description",
        "user_created",
        "user_followers",
        "user_friends",
        "user_favorites",
        "user_verified",
        "date",
        "source",
        "is_retweet",
    ]

    # Query to pass to the Stream as a filter
    query = ['pfizer', 'BioNTech', 'sinopharm', 'sinovac',
             'Moderna', 'Oxford', 'AstraZeneca', 'Covaxin', 'Sputnik V']

    # Create the path for both txt file to store detailed and general info tweets
    file = os.path.join(os.path.curdir, 'Tweepy\\tweet.txt')
    file_details = os.path.join(os.path.curdir, 'Tweepy\\tweet_detailed.txt')

    # Check if csv file exist
    # If it doesn't exist, we create a new data frame
    if os.path.exists(os.path.join(os.path.curdir, path)):
        df = pd.read_csv(path)
    else:
        df = pd.DataFrame(columns=cols)

    # Infinite loop to keep the Steam working
    while True:

        # Create the stream for the Twitter file
        twitter_streamer = TwitterStream()

        try:
            # Start the stream
            twitter_streamer.stream_tweets(file, file_details, query, df, path)
        
        except Exception as e:
            # In case of any error
            print(f"Error. Restarting Stream....\nError: {e}")
            print(e.__doc__)

