import json
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

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
        from twikit import Client
        client = Client('en-US')
        client.login(
        auth_info_1="ispet797655",
        auth_info_2="isthreeone994@gmail.com.",
        password="123abc456def")
        
        all_tweets = []
        for term in search_terms:
            tweets = client.search_tweet(term, 'Latest')
            for tweet in tweets:
                if len(all_tweets) < number_of_tweets:
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
                else:
                    break
            if len(all_tweets) >= number_of_tweets:
                break
        return all_tweets

    def tweet_cleaning(tweet):
        tweet = re.sub(r"http\S+|www\S+|https\S+", '', tweet, flags=re.MULTILINE)
        tweet = re.sub(r'@\w+', '', tweet)
        tweet = re.sub(r'#', '', tweet)
        tweet = re.sub(r'RT[\s]+', '', tweet)
        tweet = re.sub(r"[^a-zA-Z\s:;=)(]", '', tweet)
        tweet = re.sub(r'\s+', ' ', tweet).strip()
        return tweet

    @task
    def transform_tweets(all_tweets):
        geolocater = Nominatim(user_agent="ElectionSentiments")
        geocode = RateLimiter(geolocater.geocode, min_delay_seconds=1)
        nltk.download('vader_lexicon')
        sia = SentimentIntensityAnalyzer()

        tweets_df = pd.DataFrame(all_tweets)
        tweets_df['cleaned_text'] = tweets_df['text'].apply(tweet_cleaning)
        tweets_df['sentiment_score'] = tweets_df['cleaned_text'].apply(lambda text: sia.polarity_scores(text)['compound'])
        tweets_df['sentiment'] = tweets_df['sentiment_score'].apply(lambda score: 'Positive' if score >= 0.05 else 'Negative' if score <= -0.05 else 'Neutral')
        tweets_df['point'] = tweets_df['country'].apply(geocode).apply(lambda loc: f"POINT({loc.longitude} {loc.latitude})" if loc else None)

        return tweets_df

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
            bigquery.SchemaField("sentiment_score", "FLOAT"),
            bigquery.SchemaField("sentiment", "STRING"),
            bigquery.SchemaField("point", "GEOGRAPHY"),
        ]

        # Connection and client setup
        bigquery_conn_id = 'google_cloud_default'
        dataset_table = 'reddit.twitter_scraped'
        hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id, use_legacy_sql=False)
        credentials = hook.get_credentials()
        client = bigquery.Client(credentials=credentials, project=hook.project_id)
        table_id = f"{hook.project_id}.{dataset_table}"

        # Configure the load job to append data
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        # Execute the load operation
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the load job to complete

        print(f"Loaded {job.output_rows} rows into {table_id}")

    search_terms = ["election", "biden", "trump"]
    raw_data = extract_tweets_n(100, search_terms)
    processed_data = transform_tweets(raw_data)
    load_data_to_bigquery(processed_data)

# Instantiate the DAG
twitter_scrape_etl_bigquery_incremental_dag = twitter_scrape_etl_bigquery_incremental()
