from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import pandas as pd
import praw
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

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

    # Function to create and return a new Reddit client
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
    def process_data(df):
        nltk.download('vader_lexicon')
        sia = SentimentIntensityAnalyzer()

        def determine_topic(content, fallback):
            if not content.strip():  # Fallback to title if body is empty
                content = fallback
            trump_keywords = ['trump', 'donald', 'donald trump', 'president trump']
            biden_keywords = ['biden', 'joe', 'joe biden', 'president biden']
            content_lower = content.lower()
            trump_mentions = any(keyword in content_lower for keyword in trump_keywords)
            biden_mentions = any(keyword in content_lower for keyword in biden_keywords)

            if trump_mentions and biden_mentions:
                return 'Both'
            elif trump_mentions:
                return 'Trump'
            elif biden_mentions:
                return 'Biden'
            else:
                return 'None'

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

        df['topic'] = df.apply(lambda row: determine_topic(row['body'], row['title']), axis=1)
        df['sentiment_score'] = df['body'].apply(analyze_sentiment)
        df['sentiment'] = df['sentiment_score'].apply(classify_sentiment)
        
        print(df.dtypes)
        return df

    @task
    def load_data_to_bigquery(df):
        print(df.dtypes)  # Check data types before loading
        df['score'] = pd.to_numeric(df['score'], errors='coerce').fillna(0).astype(int)
        df['num_comments'] = pd.to_numeric(df['num_comments'], errors='coerce').fillna(0).astype(int)

        bigquery_conn_id = 'google_cloud_default'
        dataset_table = 'reddit.reddit_scraped'

        hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id, use_legacy_sql=False)
        credentials = hook.get_credentials()
        client = bigquery.Client(credentials=credentials, project=hook.project_id)

        job_config = bigquery.LoadJobConfig(
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
                bigquery.SchemaField("sentiment_score", "FLOAT"),
                bigquery.SchemaField("sentiment", "STRING"),
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        table_id = f"{hook.project_id}.{dataset_table}"
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the load job to complete

        print(f"Loaded {job.output_rows} rows into {table_id}")

    # Task dependencies and execution order
    raw_data = get_reddit_data()
    processed_data = process_data(raw_data)
    load_data_to_bigquery(processed_data)

# Instantiate the DAG
reddit_scrape_etl_bigquery_incremental_dag = reddit_scrape_etl_bigquery_incremental()
