from twikit import Client
from airflow.decorators import dag, task
import logging
from common.twitter_fn import _extract_tweets_n
from airflow.models import Variable
from datetime import datetime

logger = logging.getLogger(__name__)

@dag(schedule_interval='@daily', start_date=datetime(2021, 1, 1), catchup=False)
def twitter_etl():

    @task
    def print_hello():
        logging.info('Hello World')
        return 0

    @task
    def extract():
        search_params = ['Trump', 'Biden', 'US Presidential Election']
        tweets = []
        for search in search_params:
            tweets.append(_extract_tweets_n(20, search))
        return tweets
    
    @task 
    def transform(data: list):
        return 0

    task1 = print_hello()
    task2 = extract()
    task3 = transform(task2)  
    task1 >> task2 >> task3

twitter_etl_dag = twitter_etl()