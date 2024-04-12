from airflow.decorators import dag, task
import logging
from common.twitter_fn import _extract_tweets_n, _transform_tweets, _load_to_json
from datetime import datetime

logger = logging.getLogger(__name__)

@dag(schedule_interval='@daily', start_date=datetime(2021, 1, 1), catchup=False)
def twitter_etl():

    @task
    def print_hello():
        logger.info('Hello World')
        return 0

    @task
    def extract():
        search_params = ['Trump']
        tweets = []
        for search in search_params:
            tweets += (_extract_tweets_n(20, search))
        return tweets
    
    @task 
    def transform(data: list):
        return _transform_tweets(data)
    
    @task
    def load(df):
        return _load_to_json(df)

    task1 = print_hello()
    task2 = extract()
    task3 = transform(task2)
    task4 = load(task3)  
    task1 >> task2 >> task3

twitter_etl_dag = twitter_etl()