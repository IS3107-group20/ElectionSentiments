from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pynytimes import NYTAPI
import pandas as pd
import hashlib
from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import os
from dotenv import load_dotenv
import json
import hashlib
from datetime import datetime

load_dotenv()
apikey = "jp8j36ahTYcAE3AskMXyvOH0Gx9nrmch"

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to ISO format string
    raise TypeError ("Type not serializable")

def calculate_row_checksum(row):
    # Use json.dumps with a default handler for non-serializable data
    row_str = json.dumps(row, default=json_serial, sort_keys=True)
    checksum = hashlib.md5(row_str.encode()).hexdigest()
    return checksum
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
    tags=["news_dag"],
    description="DAG to scrape and retrieve data from news API"
)
def news_scrape_etl_bigquery_incremental():

    @task
    def get_nyt_articles_weekly(query):
        nytapi = NYTAPI(apikey, parse_dates=True)
        start_date = datetime.now() - timedelta(days=7)
        articles = nytapi.article_search(
            query=query,
            options={
                "sort": "relevance",
                "sources": ["New York Times", "AP", "Reuters", "International Herald Tribune"],
            },
            dates={"begin": start_date, "end": datetime.now()},
            results=10,
        )
        return articles
    
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
        new_data = df[~df['checksum'].isin(existing_ids)]
        return new_data

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
            "checksum": [calculate_row_checksum(article) for article in articles]
        }
        df = pd.DataFrame(data)
        return df

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
                bigquery.SchemaField("checksum", "STRING", description="Primary Key")
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        table_ref = f"{hook.project_id}.{dataset_id}.{table_id}"
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Loaded {job.output_rows} rows into {table_ref}")

    # Combine data and load to BigQuery
    @task
    def combine_and_load(df1, df2):
        return pd.concat([df1, df2], axis=0)

    # Task chaining should be done using outputs of previous tasks
    articles_trump = get_nyt_articles_weekly("trump")
    articles_biden = get_nyt_articles_weekly("biden")
    news_data_trump = transform_into_csv(articles_trump)
    news_data_biden = transform_into_csv(articles_biden)
    news_df = combine_and_load(news_data_trump, news_data_biden)
    existing_ids = get_existing_ids()
    new_data = filter_new_data(news_df, existing_ids)
    load_data_to_bigquery(new_data)

news_scrape_etl_bigquery_incremental_dag = news_scrape_etl_bigquery_incremental()