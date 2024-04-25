from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import pandas as pd
import praw
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import re
import numpy as np
from common.helper import bq_load_data, extract_aspects, get_existing_ids_by_source, update_topic_sentiment
from common.reddit_common import determine_topic, clean_and_lemmatize, classify_sentiment

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

@dag(dag_id='reddit_scrape', default_args=default_args, schedule_interval='@daily', catchup=False, tags=['reddit', 'bigquery'])
def reddit_scrape_etl_bigquery_incremental():

    def get_reddit_client():
        return praw.Reddit(client_id='9Vy8b4OZfZhDHn0bD4Q94w', 
                           client_secret='h-JRuPyJJBWrWT8qnrt9PCL2V39RbA', 
                           user_agent='is3107')

    @task
    def get_reddit_data():
        reddit = get_reddit_client()
        subreddits = ['2024elections', 'JoeBiden', 'trump', '2024Election']
        all_posts = []
        
        for subreddit_name in subreddits:
            subreddit = reddit.subreddit(subreddit_name)
            for post in subreddit.hot(limit=None):
                all_posts.append([
                    post.title, post.score, post.id, str(post.subreddit), post.url,
                    post.num_comments, post.selftext, datetime.fromtimestamp(post.created).isoformat()
                ])
        
        df = pd.DataFrame(all_posts, columns=[
            'title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created'
        ])
        return df

    @task
    def get_existing_ids():
        return get_existing_ids_by_source('reddit_scraped')

    @task
    def filter_new_data(df, existing_ids):
        new_data = df[~df['id'].isin(existing_ids)]
        return new_data

    @task
    def process_data(df):
        nltk.download('vader_lexicon')
        nltk.download('wordnet')
        nltk.download('stopwords')

        # Initialize Sentiment Intensity Analyzer and Word Lemmatizer
        sia = SentimentIntensityAnalyzer()
        lemmatizer = WordNetLemmatizer()
        stop_words = set(stopwords.words('english'))


        def analyze_sentiment(text):
            sentiment_scores = sia.polarity_scores(text)
            return sentiment_scores['compound']
        
        # apply to df
        df['cleaned_text'] = df.apply(lambda x: clean_and_lemmatize(x['title'] + " " + x['body'], lemmatizer, stop_words), axis=1)
        df['topic'] = df.apply(lambda row: determine_topic(row['cleaned_text'], row['title']), axis=1)
        df['sentiment_score'] = df['cleaned_text'].apply(analyze_sentiment)
        df['aspect_sentiments'] = df.apply(lambda row: extract_aspects(row['cleaned_text']) if row['topic'] == 'Both' else np.nan,axis=1)
        df['sentiment'] = df['sentiment_score'].apply(classify_sentiment)

        # Update topics based on aspect sentiments
        new_rows = update_topic_sentiment(df)

        new_df = pd.DataFrame(new_rows)
        new_df = new_df.drop('aspect_sentiments', axis = 1)
        new_df = new_df.reset_index(drop=True)
        new_df['score'] = pd.to_numeric(new_df['score'], errors='coerce').fillna(0).astype(int)
        new_df['num_comments'] = pd.to_numeric(new_df['num_comments'], errors='coerce').fillna(0).astype(int)
        new_df['sentiment'] = new_df['sentiment'].str.lower()
        new_df['topic'] = new_df['topic'].str.lower()
        return new_df

    @task
    def load_data_to_bigquery(df):
        schema=[
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("score", "INTEGER"),
                bigquery.SchemaField("id", "STRING"),
                bigquery.SchemaField("subreddit", "STRING"),
                bigquery.SchemaField("url", "STRING"),
                bigquery.SchemaField("num_comments", "INTEGER"),
                bigquery.SchemaField("body", "STRING"),
                bigquery.SchemaField("created", "STRING"),
                bigquery.SchemaField("topic", "STRING"),
                bigquery.SchemaField("cleaned_text", "STRING"),
                bigquery.SchemaField("sentiment_score", "FLOAT"),
                bigquery.SchemaField("sentiment", "STRING"),
            ]
        bq_load_data(df,schema,"reddit.reddit_scraped")

    # Task dependencies and execution order
    raw_data = get_reddit_data()
    existing_ids = get_existing_ids()
    processed_data = process_data(raw_data)
    new_data = filter_new_data(processed_data, existing_ids)
    load_data_to_bigquery(new_data)

# Instantiate the DAG
reddit_scrape_etl_bigquery_incremental_dag = reddit_scrape_etl_bigquery_incremental()
