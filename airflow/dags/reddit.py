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


        # apply to df
        df['cleaned text'] = df.apply(lambda x: clean_and_lemmatize(x['title'] + " " + x['body']), axis=1)
        df['topic'] = df.apply(lambda row: determine_topic(row['cleaned text'], row['title']), axis=1)
        df['sentiment_score'] = df['cleaned text'].apply(analyze_sentiment)
        df['aspect_sentiments'] = df.apply(lambda row: extract_aspects(row['cleaned_text']) if row['topic'] == 'Both' else np.nan,axis=1)
        df['sentiment'] = df['sentiment_score'].apply(classify_sentiment)

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
                    new_row['topic'] = 'Trump' if aspect == 'trump' else 'Biden'
                    new_row['sentiment'] = max_sentiment
                    new_row['sentiment_score'] = max_score
                    new_rows.append(new_row)
            else:
                new_rows.append(row)

        new_df = pd.DataFrame(new_rows)
        new_df = new_df.drop('aspect_sentiments', axis = 1)
        new_df = new_df.reset_index(drop=True)

        return new_df

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
                bigquery.SchemaField("cleaned_text", "STRING"),
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
