from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
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
            for post in subreddit.hot(limit=None):  # Fetch all posts
                all_posts.append([
                    post.title, post.score, post.id, str(post.subreddit), post.url,
                    post.num_comments, post.selftext, datetime.fromtimestamp(post.created)
                ])
        
        # Create DataFrame from the list of posts
        df = pd.DataFrame(all_posts, columns=[
            'title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created'
        ])
        return df

    @task
    def process_data(df):
        nltk.download('vader_lexicon')
        sia = SentimentIntensityAnalyzer()

        # Function to determine the topic based on keywords in the content
        def determine_topic(content):
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

        # Function to analyze the sentiment of the content
        def analyze_sentiment(text):
            sentiment_scores = sia.polarity_scores(text)
            return sentiment_scores['compound']

        # Function to classify the sentiment based on the compound score
        def classify_sentiment(score):
            if score >= 0.05:
                return 'Positive'
            elif score <= -0.05:
                return 'Negative'
            else:
                return 'Neutral'

        # Apply the functions to process the DataFrame
        df['topic'] = df['body'].apply(determine_topic)
        df['sentiment_score'] = df['body'].apply(analyze_sentiment)
        df['sentiment'] = df['sentiment_score'].apply(classify_sentiment)
        
        return df

    @task
    def load_data_to_bigquery(df):
        bigquery_conn_id = 'your_bigquery_connection'
        dataset_table = 'reddit.reddit_scraped'
        project_id = 'is3107-project-419009'

        # Append processed data to BigQuery table
        df.to_gbq(
            destination_table=dataset_table,        
            project_id=project_id,
            if_exists='append'
        )

    # Task dependencies and execution order
    raw_data = get_reddit_data()
    processed_data = process_data(raw_data)
    load_data_to_bigquery(processed_data)

# Instantiate the DAG
reddit_scrape_etl_bigquery_incremental_dag = reddit_scrape_etl_bigquery_incremental()
