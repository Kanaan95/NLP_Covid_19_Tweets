from Twitter import *
import pandas as pd
import tweepy
import os
from datetime import datetime

if __name__ == '__main__':


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
        "language"
    ]

    # Query to pass to the Stream as a filter
    query = ['pfizer', 'BioNTech', 'sinopharm', 'sinovac',
             'Moderna', 'Oxford', 'AstraZeneca', 'Covaxin', 'SputnikV']

    
    today = datetime.now().strftime("%d-%m-%Y")

    if not os.path.exists(os.path.join("D:\Twitter_Data_Vaccines", today)):
        os.makedirs(os.path.join("D:\Twitter_Data_Vaccines", today))

    # Create the path for both txt file to store detailed and general info tweets
    file = os.path.join("D:\\Twitter_Data_Vaccines", today,'tweet.txt')

    # Check if csv file exist
    # If it doesn't exist, we create a new data frame
    df_path = os.path.join("D:\\Twitter_Data_Vaccines", "tweets.csv")
    if os.path.exists(df_path):
        df = pd.read_csv(df_path)
    else:
        df = pd.DataFrame(columns=cols)

    # Infinite loop to keep the Steam working
    while True:

        # Create the stream for the Twitter file
        twitter_streamer = TwitterStream()

        try:
            # Start the stream
            # twitter_streamer.stream_tweets(file, file_details, query, df, path)
            twitter_streamer.stream_tweets(file, df, df_path, query)
        
        except Exception as e:
            # In case of any error
            print(f"Error. Restarting Stream....\nError: {e}")
            # print(e.__doc__)

