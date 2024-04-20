from twikit import Client
import pandas as pd

def get_twitter_data():
        client = Client('en-US')
        client.login(
            auth_info_1="ispet797655",
            auth_info_2="isthreeone994@gmail.com.",
            password="123abc456def"
        )
        
        search_terms = ["election", "biden", "trump"]
        all_tweets = []
        for term in search_terms:
            tweets = client.search_tweet(term, 'Latest')
            for tweet in tweets:
                all_tweets.append({
                    'id': tweet.id,
                    'text': tweet.text,
                    'lang': tweet.lang,
                    'created_at': tweet.created_at,
                    'user': tweet.user.screen_name,
                    'score': tweet.favorite_count,
                    'num_comments': tweet.reply_count
                })
        print(all_tweets)
    
get_twitter_data()