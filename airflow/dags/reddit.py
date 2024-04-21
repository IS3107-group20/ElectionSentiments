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

    @task
    def get_reddit_client():
        return praw.Reddit(client_id='your_client_id', 
                           client_secret='your_client_secret', 
                           user_agent='your_user_agent')

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
        bigquery_conn_id = 'google_cloud_default'
        hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id, use_legacy_sql=False)
        client = bigquery.Client(credentials=hook.get_credentials(), project=hook.project_id)
        query = "SELECT id FROM `is3107-project-419009.reddit.reddit_scraped`"
        query_job = client.query(query)
        results = query_job.result()
        existing_ids = {row.id for row in results}
        return existing_ids

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

        def clean_and_lemmatize(text):
            text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
            text = re.sub(r'@\w+', '', text)
            text = re.sub(r'#', '', text)
            text = re.sub(r'RT[\s]+', '', text)
            text = re.sub(r"[^a-zA-Z\s]", ' ', text)
            text = re.sub(r'\s+', ' ', text).strip()
            # Lemmatization
            words = text.lower().split()
            lemmatized_words = [lemmatizer.lemmatize(word) for word in words if word not in stop_words]
            return ' '.join(lemmatized_words)

        def determine_topic(content, fallback):
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

            if not content.strip():
                content = fallback
            
            content_lower = content.lower()
            trump_count = sum(content_lower.count(keyword) for keyword in trump_keywords)
            biden_count = sum(content_lower.count(keyword) for keyword in biden_keywords)

            if trump_count > biden_count:
                return 'Trump'
            elif biden_count > trump_count:
                return 'Biden'
            else:
                return 'Both' if trump_count > 0 else 'None'

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

        # apply to df
        df['cleaned text'] = df.apply(lambda x: clean_and_lemmatize(x['title'] + " " + x['body']), axis=1)
        df['topic'] = df.apply(lambda row: determine_topic(row['cleaned text'], row['title']), axis=1)
        df['sentiment_score'] = df['cleaned text'].apply(analyze_sentiment)
        df['sentiment'] = df['sentiment_score'].apply(classify_sentiment)

        return df

    @task
    def load_data_to_bigquery(df):
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
        job.result()

        print(f"Loaded {job.output_rows} rows into {table_id}")

    # Task dependencies and execution order
    raw_data = get_reddit_data()
    existing_ids = get_existing_ids()
    processed_data = process_data(raw_data)
    new_data = filter_new_data(processed_data, existing_ids)
    load_data_to_bigquery(new_data)

# Instantiate the DAG
reddit_scrape_etl_bigquery_incremental_dag = reddit_scrape_etl_bigquery_incremental()
