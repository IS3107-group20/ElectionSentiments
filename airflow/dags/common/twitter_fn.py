import logging
from twikit import Client
from airflow.models import Variable

logger = logging.getLogger(__name__)


def _extract_tweets_n(number_of_tweets: int, search: str):
    USERNAME = Variable.get('TWITTER_USERNAME')
    EMAIL = Variable.get('TWITTER_EMAIL')
    PASSWORD = Variable.get('TWITTER_PASSWORD')

    client = Client('en-US')
    client.login(
        auth_info_1=USERNAME,
        auth_info_2=EMAIL,
        password=PASSWORD
    )
    all_tweets = []
    tweets = client.search_tweet(search, 'Latest')
    while len(all_tweets) < number_of_tweets:
        for tweet in tweets:
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
            }`
            all_tweets.append(curr_tweet)
            if len(all_tweets) == number_of_tweets:
                break
        tweets.next()
    logger.info('Tweets extracted, count: %s', len(all_tweets))
    return all_tweets


    
