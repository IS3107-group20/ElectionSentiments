from twikit import Client
from airflow.decorators import dag, task
import logging
from airflow.models import Variable
from datetime import datetime

@dag(schedule_interval='@daily', start_date=datetime(2021, 1, 1), catchup=False)
def twitter_etl():

    @task
    def print_hello():
        logging.info('Hello World')
        return 0

    @task
    def extract():
        # Enter your account information
        USERNAME = Variable.get('TWITTER_USERNAME')
        EMAIL = Variable.get('TWITTER_EMAIL')
        PASSWORD = Variable.get('TWITTER_PASSWORD')

        client = Client('en-US')
        client.login(
            auth_info_1=USERNAME,
            auth_info_2=EMAIL,
            password=PASSWORD
        )
        tweets = client.search_tweet('Trump', 'Top')
        logging.info('Tweets extracted, count: %s', len(tweets))
        for tweet in tweets:
            logging.info('Tweet: %s', tweet)
            logging.info('Tweet text: %s', tweet.text)
        return 0

    task1 = print_hello()
    task2 = extract()
    task1 >> task2

twitter_etl_dag = twitter_etl()