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
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import torch.nn.functional as F

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
    
    @task
    def get_existing_ids():
        bigquery_conn_id = 'google_cloud_default'
        hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id, use_legacy_sql=False)
        client = bigquery.Client(credentials=hook.get_credentials(), project=hook.project_id)
        query = "SELECT id FROM `is3107-project-419009.reddit.twitter_scraped`"
        query_job = client.query(query)
        results = query_job.result()
        existing_ids = {row.id for row in results}
        return existing_ids

    @task
    def filter_new_data(df, existing_ids):
        new_data = df[~df['id'].isin(existing_ids)]
        return new_data

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
        
        trump_keywords = [
        'trump', 'donald', 'donald trump', 'president trump', 'trump administration',
        'trump campaign', 'trump era', 'ivanka', 'melania', 'trump policies', 'maga',
        'make america great again', 'trump supporter', 'trump rally', 'trump impeachment'
        ]
        
        biden_keywords = [
            'biden', 'joe', 'joe biden', 'president biden', 'biden administration',
            'biden campaign', 'biden era', 'hunter biden', 'jill biden', 'biden policies',
            'build back better', 'biden supporter', 'biden rally', 'biden impeachment'
        ]

        def classify_topic(text):
            text_lower = text.lower()
            trump_count = sum(text_lower.count(keyword) for keyword in trump_keywords)
            biden_count = sum(text_lower.count(keyword) for keyword in biden_keywords)

            if trump_count > biden_count:
                return 'Trump'
            elif biden_count > trump_count:
                return 'Biden'
            return 'Both' if trump_count > 0 and biden_count > 0 else 'None'
        
        absa_tokenizer = AutoTokenizer.from_pretrained("yangheng/deberta-v3-base-absa-v1.1")
        absa_model = AutoModelForSequenceClassification.from_pretrained("yangheng/deberta-v3-base-absa-v1.1")
        
        def extract_aspects(text):
            aspects = ['trump', 'biden'] 
            aspect_sentiments = {}
            if text.strip():
                for aspect in aspects:
                    inputs = absa_tokenizer(f"[CLS] {text} [SEP] {aspect} [SEP]", return_tensors="pt")
                    outputs = absa_model(**inputs)
                    probs = F.softmax(outputs.logits, dim=1)
                    probs = probs.detach().numpy()[0]
                    aspect_sentiments[aspect] = {label: prob for prob, label in zip(probs, ["negative", "neutral", "positive"])}
            return aspect_sentiments

        tweets_df = pd.DataFrame(all_tweets)
        tweets_df['cleaned_text'] = tweets_df['text'].apply(tweet_cleaning)
        tweets_df['topic'] = tweets_df['cleaned_text'].apply(classify_topic)
        tweets_df['sentiment_score'] = tweets_df['cleaned_text'].apply(lambda text: sia.polarity_scores(text)['compound'])
        tweets_df['sentiment'] = tweets_df['sentiment_score'].apply(lambda score: 'Positive' if score >= 0.05 else 'Negative' if score <= -0.05 else 'Neutral')
        tweets_df['point'] = tweets_df['country'].apply(geocode).apply(lambda loc: f"POINT({loc.longitude} {loc.latitude})" if loc else None)

        # Update topics based on aspect sentiments
        def update_topic_based_on_aspects(row):
            if row['topic'] == 'Both':
                aspect_results = []
                for aspect, sentiments in row['aspect_sentiments'].items():
                    most_likely_sentiment = max(sentiments, key=sentiments.get)
                    aspect_results.append(f"{aspect}: {most_likely_sentiment}")
                return ', '.join(aspect_results)
            return row['topic']

        tweets_df['topic'] = tweets_df.apply(update_topic_based_on_aspects, axis=1)

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
            bigquery.SchemaField("topic", "STRING"),
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

    search_terms = ["US Elections", "biden", "trump"]
    raw_data = extract_tweets_n(100, search_terms)
    processed_data = transform_tweets(raw_data)
    existing_ids = get_existing_ids()
    new_data = filter_new_data(processed_data, existing_ids)
    load_data_to_bigquery(new_data)

# Instantiate the DAG
twitter_scrape_etl_bigquery_incremental_dag = twitter_scrape_etl_bigquery_incremental()
