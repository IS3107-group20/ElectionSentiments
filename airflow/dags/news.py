import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pynytimes import NYTAPI
import pandas as pd
import hashlib
import numpy as np
import json
import re
from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from common.helper import bq_load_data, extract_aspects, get_existing_ids_by_source, update_topic_sentiment
from common.news_common import calculate_row_checksum, classify_topic, clean_text, classify_sentiment
from airflow.models import Variable


apikey = Variable.get("NEWS_API_KEY")


nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

def analyze_sentiment(text):
    """Analyze the sentiment of the text and return the compound sentiment score"""
    return sia.polarity_scores(text)['compound']

@dag(
    dag_id="news_scrape",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2023, 12, 4),
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 2, 28),
    dagrun_timeout=timedelta(minutes=5),
    catchup=False,
    tags=["news", "bigquery"],
    description="DAG to scrape and retrieve data from news API and load to BigQuery"
)
def news_scrape_etl_bigquery_incremental():

    @task
    def get_nyt_articles_weekly(query):
        nytapi = NYTAPI(apikey, parse_dates=True)
        start_date = datetime.now() - timedelta(days=7)
        articles = nytapi.article_search(
            query=query,
            options={"sort": "relevance", "sources": ["New York Times", "AP", "Reuters", "International Herald Tribune"]},
            dates={"begin": start_date, "end": datetime.now()},
            results=10,
        )
        return articles

    @task
    def transform_into_csv(articles):
        data = {
            "headline": [article["headline"]["main"] for article in articles],
            "author": [article["headline"]["kicker"] for article in articles],
            "lead_paragraph": [article["lead_paragraph"] for article in articles],
            "snippet": [article["snippet"] for article in articles],
            "source": [article["source"] for article in articles],
            "publication_date": [article["pub_date"] for article in articles],
            "keywords": ["; ".join(keyword["value"] for keyword in article["keywords"]) for article in articles],
            "cleaned_text": [clean_text(article["headline"]["main"] + " " + article["lead_paragraph"] + " " + article["snippet"]) for article in articles],
            "sentiment_score": [analyze_sentiment(clean_text(article["headline"]["main"] + " " + article["lead_paragraph"] + " " + article["snippet"])) for article in articles],
            "checksum": [calculate_row_checksum(article) for article in articles]
        }
        
        df = pd.DataFrame(data)
        df['sentiment'] = df['sentiment_score'].apply(classify_sentiment)
        df['topic'] = df['keywords'].apply(classify_topic)
        df['aspect_sentiments'] = df.apply(lambda row: extract_aspects(row['cleaned_text']) if row['topic'] == 'Both' else np.nan,axis=1)
        
        # Update topics based on aspect sentiments
        new_rows = update_topic_sentiment(df)

        new_df = pd.DataFrame(new_rows)
        new_df = new_df.drop('aspect_sentiments', axis = 1)
        new_df = new_df.reset_index(drop=True)
        new_df['sentiment'] = new_df['sentiment'].str.lower()
        new_df['topic'] = new_df['topic'].str.lower()
        return new_df
    
    @task
    def get_existing_ids():
        return get_existing_ids_by_source("news_scraped")

    @task
    def filter_new_data(df, existing_ids):
        return df[~df['checksum'].isin(existing_ids)]

    @task
    def load_data_to_bigquery(df):
        schema=[
                bigquery.SchemaField("headline", "STRING"),
                bigquery.SchemaField("author", "STRING"),
                bigquery.SchemaField("lead_paragraph", "STRING"),
                bigquery.SchemaField("snippet", "STRING"),
                bigquery.SchemaField("source", "STRING"),
                bigquery.SchemaField("publication_date", "TIMESTAMP"),
                bigquery.SchemaField("keywords", "STRING"),
                bigquery.SchemaField("topic", "STRING"),
                bigquery.SchemaField("checksum", "STRING", description="Primary Key"),
                bigquery.SchemaField("cleaned_text", "STRING"),
                bigquery.SchemaField("sentiment_score", "FLOAT"),
                bigquery.SchemaField("sentiment", "STRING"),
            ]
        bq_load_data(df,schema, "reddit.news_scraped")

    @task
    def combine_and_load(df1, df2):
        return pd.concat([df1, df2], axis=0)

    articles_trump = get_nyt_articles_weekly("trump")
    articles_biden = get_nyt_articles_weekly("biden")
    news_data_trump = transform_into_csv(articles_trump)
    news_data_biden = transform_into_csv(articles_biden)
    news_df = combine_and_load(news_data_trump, news_data_biden)
    existing_ids = get_existing_ids()
    new_data = filter_new_data(news_df, existing_ids)
    load_data_to_bigquery(new_data)

news_scrape_etl_bigquery_incremental_dag = news_scrape_etl_bigquery_incremental()
