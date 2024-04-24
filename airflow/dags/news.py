import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pynytimes import NYTAPI
import pandas as pd
import hashlib
import json
import re
from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import torch.nn.functional as F

apikey = "jp8j36ahTYcAE3AskMXyvOH0Gx9nrmch" 

nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def calculate_row_checksum(row):
    """Calculate a checksum for a row to identify unique articles"""
    row_str = json.dumps(row, default=json_serial, sort_keys=True)
    checksum = hashlib.md5(row_str.encode()).hexdigest()
    return checksum

def clean_text(text):
    """Clean text by removing URLs, special characters, and normalizing whitespace"""
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#', '', text)
    text = re.sub(r'RT[\s]+', '', text)
    text = re.sub(r"[^a-zA-Z\s]", ' ', text)
    text = re.sub(r'\s+', ' ', text).strip().lower()
    return text

def classify_topic(keywords_str):
    # Normalize the string by converting to lowercase and splitting on semicolon
    # and strip spaces to ensure consistent keyword matching.
    keywords_list = [keyword.strip().lower() for keyword in keywords_str.split(';')]
    
    trump_keywords = {
        'trump', 'donald', 'donald trump', 'president trump', 'trump administration',
        'trump campaign', 'trump era', 'ivanka', 'melania', 'trump policies', 'maga',
        'make america great again', 'trump supporter', 'trump rally', 'trump impeachment'
    }
    biden_keywords = {
        'biden', 'joe', 'joe biden', 'president biden', 'biden administration',
        'biden campaign', 'biden era', 'hunter biden', 'jill biden', 'biden policies',
        'build back better', 'biden supporter', 'biden rally', 'biden impeachment'
    }

    # Simplified keyword matching for broader matches
    trump_count = sum(any(tk in keyword for tk in trump_keywords) for keyword in keywords_list)
    biden_count = sum(any(bk in keyword for bk in biden_keywords) for keyword in keywords_list)

    if trump_count > biden_count:
        return 'Trump'
    elif biden_count > trump_count:
        return 'Biden'
    return 'Both' if trump_count > 0 and biden_count > 0 else 'None'

def analyze_sentiment(text):
    """Analyze the sentiment of the text and return the compound sentiment score"""
    return sia.polarity_scores(text)['compound']

def classify_sentiment(score):
        if score >= 0.05:
            return 'Positive'
        elif score <= -0.05:
            return 'Negative'
        else:
            return 'Neutral'

@dag(
    dag_id="news_dag",
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
    tags=["news"],
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
        
        df = pd.DataFrame(data)
        df['sentiment'] = df['sentiment_score'].apply(classify_sentiment)
        df['topic'] = df['keywords'].apply(classify_topic)
        df['aspect_sentiments'] = df['cleaned_text'].apply(extract_aspects)
        
        # Update topics based on aspect sentiments
        def get_max_sentiment(sentiments):
            max_sentiment = max(sentiments, key=sentiments.get)
            max_score = sentiments[max_sentiment]
            return max_sentiment, max_score

        new_rows = []

        for index, row in df.iterrows():
            if row['topic'] == 'Both':
                for aspect, sentiments in row['aspect_sentiments'].items():
                    max_sentiment, max_score = get_max_sentiment(sentiments)
                    new_row = row.copy()
                    new_row['topic'] = aspect
                    new_row['sentiment'] = max_sentiment
                    new_row['sentiment_score'] = max_score
                    new_rows.append(new_row)
            else:
                new_rows.append(row)

        new_df = pd.DataFrame(new_rows)

        new_df = new_df.reset_index(drop=True)

        return new_df
    
    @task
    def get_existing_ids():
        bigquery_conn_id = 'google_cloud_default'
        hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id, use_legacy_sql=False)
        client = bigquery.Client(credentials=hook.get_credentials(), project=hook.project_id)
        query = "SELECT checksum FROM `is3107-project-419009.reddit.news_scraped`"
        query_job = client.query(query)
        results = query_job.result()
        existing_ids = {row.checksum for row in results}
        return existing_ids

    @task
    def filter_new_data(df, existing_ids):
        return df[~df['checksum'].isin(existing_ids)]

    @task
    def load_data_to_bigquery(df):
        bigquery_conn_id = 'google_cloud_default'
        hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id, use_legacy_sql=False)
        client = hook.get_client()
        dataset_id = "reddit"
        table_id = "news_scraped"

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("headline", "STRING"),
                bigquery.SchemaField("author", "STRING"),
                bigquery.SchemaField("lead_paragraph", "STRING"),
                bigquery.SchemaField("snippet", "STRING"),
                bigquery.SchemaField("source", "STRING"),
                bigquery.SchemaField("publication_date", "DATE"),
                bigquery.SchemaField("keywords", "STRING"),
                bigquery.SchemaField("topic", "STRING"),
                bigquery.SchemaField("checksum", "STRING", description="Primary Key"),
                bigquery.SchemaField("cleaned_text", "STRING"),
                bigquery.SchemaField("sentiment_score", "FLOAT"),
                bigquery.SchemaField("sentiment", "STRING"),
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        table_ref = f"{hook.project_id}.{dataset_id}.{table_id}"
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Loaded {job.output_rows} rows into {table_ref}")
        
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
