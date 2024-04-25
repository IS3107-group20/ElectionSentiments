import json
import numpy as np
import time
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from common.helper import bq_load_data, extract_aspects, get_existing_ids_by_source, update_topic_sentiment
from common.twitter_common import classify_topic, tweet_cleaning, extract_helper

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 4),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='twitter_scrape', default_args=default_args, schedule_interval='@daily', catchup=False, tags=['twitter', 'bigquery'])
def twitter_scrape_etl_bigquery_incremental():
    
    @task
    def extract_tweets_n(number_of_tweets: int, search_terms: list):
        return extract_helper(number_of_tweets,search_terms)
    
    @task
    def get_existing_ids():
        return get_existing_ids_by_source("twitter_scraped")

    @task
    def filter_new_data(df, existing_ids):
        new_data = df[~df['id'].isin(existing_ids)]
        return new_data

    @task
    def transform_tweets(all_tweets):
        geolocater = Nominatim(user_agent="ElectionSentiments")
        geocode = RateLimiter(geolocater.geocode, min_delay_seconds=1)
        nltk.download('vader_lexicon')
        sia = SentimentIntensityAnalyzer()
        

        tweets_df = pd.DataFrame(all_tweets)
        tweets_df = tweets_df.drop_duplicates(subset='id')
        tweets_df['cleaned_text'] = tweets_df['text'].apply(tweet_cleaning)
        tweets_df['topic'] = tweets_df['cleaned_text'].apply(classify_topic)
        tweets_df['sentiment_score'] = tweets_df['cleaned_text'].apply(lambda text: sia.polarity_scores(text)['compound'])
        tweets_df['sentiment'] = tweets_df['sentiment_score'].apply(lambda score: 'Positive' if score >= 0.05 else 'Negative' if score <= -0.05 else 'Neutral')
        tweets_df['point'] = tweets_df['country'].apply(lambda country: geocode(country) if pd.notna(country) and country.strip() != "" else None).apply(lambda loc: f"POINT({loc.longitude} {loc.latitude})" if loc else None)
        tweets_df['aspect_sentiments'] = tweets_df.apply(lambda row: extract_aspects(row['cleaned_text']) if row['topic'] == 'Both' else np.nan,axis=1)

        # Update topics based on aspect sentiments
        new_rows = update_topic_sentiment(tweets_df)

        new_df = pd.DataFrame(new_rows)
        new_df = new_df.drop('aspect_sentiments', axis = 1)
        new_df = new_df.reset_index(drop=True)
        new_df['sentiment'] = new_df['sentiment'].str.lower()
        new_df['topic'] = new_df['topic'].str.lower()
        return new_df

    @task
    def load_data_to_bigquery(df):
        print(df.dtypes)  # Check data types before loading

        # Define the BigQuery schema
        schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("lang", "STRING"),
            bigquery.SchemaField("created_at_datetime", "DATETIME"),
            bigquery.SchemaField("user", "STRING"),
            bigquery.SchemaField("quote_count", "INTEGER"),
            bigquery.SchemaField("favorite_count", "INTEGER"),
            bigquery.SchemaField("reply_count", "INTEGER"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("cleaned_text", "STRING"),
            bigquery.SchemaField("topic", "STRING"),
            bigquery.SchemaField("sentiment_score", "FLOAT"),
            bigquery.SchemaField("sentiment", "STRING"),
            bigquery.SchemaField("point", "GEOGRAPHY"),
        ]
        bq_load_data(df,schema,"reddit.twitter_scraped")



    search_terms = ["US Elections 2024", "biden", "trump"]
    raw_data = extract_tweets_n(100, search_terms)
    processed_data = transform_tweets(raw_data)
    existing_ids = get_existing_ids()
    new_data = filter_new_data(processed_data, existing_ids)
    load = load_data_to_bigquery(new_data)

# Instantiate the DA
twitter_scrape_etl_bigquery_incremental_dag = twitter_scrape_etl_bigquery_incremental()
