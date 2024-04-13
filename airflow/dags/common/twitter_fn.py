import logging
import re
import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from twikit import Client
from airflow.models import Variable

logger = logging.getLogger(__name__)


def _extract_tweets_n(number_of_tweets: int, search: str):
    USERNAME = Variable.get('TWITTER_USERNAME')
    EMAIL = Variable.get('TWITTER_EMAIL')
    PASSWORD = Variable.get('TWITTER_PASSWORD')

    client = Client('en-US')
    client.login(
        auth_info_1=USERNAME,
        auth_info_2=EMAIL,
        password=PASSWORD
    )
    all_tweets = []
    tweets = client.search_tweet(search, 'Latest')
    while len(all_tweets) < number_of_tweets:
        for tweet in tweets:
            curr_tweet = {
                'id': tweet.id,
                'text': tweet.text,
                'lang': tweet.lang,
                'created_at_datetime': tweet.created_at_datetime,
                'user': tweet.user.screen_name,
                'quote_count': tweet.quote_count,
                'favorite_count': tweet.favorite_count,
                'reply_count': tweet.reply_count,
                'country' : tweet.user.location
            }
            all_tweets.append(curr_tweet)
            if len(all_tweets) == number_of_tweets:
                break
        tweets.next()
    logger.info('Tweets extracted, count: %s', len(all_tweets))
    return all_tweets


def tweet_cleaning(tweet):
    tweet = re.sub(r"http\S+|www\S+|https\S+", '', tweet, flags=re.MULTILINE)
    tweet = re.sub(r'@\w+', '', tweet)
    tweet = re.sub(r'#', '', tweet)
    tweet = re.sub(r'RT[\s]+', '', tweet)
    tweet = re.sub(r"[^a-zA-Z\s:;=)(]", '', tweet)
    tweet = re.sub(r'\s+', ' ', tweet).strip()
    
    return tweet

def _transform_tweets(all_tweets):
    # Geocoding
    geolocater = Nominatim(user_agent = "ElectionSentiments")
    geocode = RateLimiter(geolocater.geocode, min_delay_seconds = 1)
    # Sentiment Analysis
    nltk.download('vader_lexicon')
    sia = SentimentIntensityAnalyzer()
    # Convert to dataframe
    tweets_df = pd.DataFrame(all_tweets)
    
    def analyze_sentiment(text):
        sentiment_scores = sia.polarity_scores(text)
        return sentiment_scores['compound']
        
    def classify_sentiment(score):
        if score >= 0.05:
            return 'Positive'
        elif score <= -0.05:
            return 'Negative'
        else:
            return 'Neutral'
    tweets_df['cleaned_text'] = tweets_df['text'].apply(tweet_cleaning)
    tweets_df['sentiment_score'] = tweets_df['cleaned_text'].apply(analyze_sentiment)
    tweets_df['sentiment'] = tweets_df['sentiment_score'].apply(classify_sentiment)
    tweets_df['point'] = tweets_df['country'].apply(geocode).apply(lambda loc: tuple(loc.point) if loc else None)
    logger.info(tweets_df['country'])
    logger.info(tweets_df.dtypes)
    return tweets_df


def _load_to_json(df):
    json_result = df.to_json(orient='records', lines=True)
    logger.info(json_result)
    return json_result

    
