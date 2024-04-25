import re
import time
from airflow.models import Variable
def classify_topic(text):
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
    text_lower = text.lower()
    trump_count = sum(text_lower.count(keyword) for keyword in trump_keywords)
    biden_count = sum(text_lower.count(keyword) for keyword in biden_keywords)

    if trump_count > biden_count:
        return 'Trump'
    elif biden_count > trump_count:
        return 'Biden'
    return 'Both' if trump_count > 0 and biden_count > 0 else 'None'

def tweet_cleaning(tweet):
    tweet = re.sub(r"http\S+|www\S+|https\S+", '', tweet, flags=re.MULTILINE)
    tweet = re.sub(r'@\w+', '', tweet)
    tweet = re.sub(r'#', '', tweet)
    tweet = re.sub(r'RT[\s]+', '', tweet)
    tweet = re.sub(r"[^a-zA-Z\s:;=)(]", '', tweet)
    tweet = re.sub(r'\s+', ' ', tweet).strip()
    return tweet

def extract_helper(number_of_tweets,search_terms):
    from twikit import Client,errors
    #Change Credentials
    client = Client('en-US')
    client.login(
        auth_info_1=Variable.get("TWITTER_USERNAME"),
        auth_info_2=Variable.get("TWITTER_EMAIL"),
        password=Variable.get("TWITTER_PASSWORD"))
    search_terms = ['Trump' , 'Biden']
    all_tweets = []
    for term in search_terms:
        while True:
            try:
                tweets = client.search_tweet(term, 'Top')
                topic_tweets=[]
                while True:
                    try:
                        for i, tweet in enumerate(tweets):
                            if len(topic_tweets) < number_of_tweets:
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
                                }
                                topic_tweets.append(curr_tweet)
                            else:
                                break
                        if len(topic_tweets) >= number_of_tweets:
                            break
                        tweets = tweets.next()
                    except errors.TooManyRequests as e:
                        retry_after = 100
                        print(f"Rate limit hit. Pausing for {retry_after} seconds.")
                        time.sleep(retry_after)
                all_tweets += topic_tweets
                print("All Tweets Length: ", len(all_tweets))
                break
            except errors.TooManyRequests as e:
                retry_after = 100
                print(f"Rate limit hit. Pausing for {retry_after} seconds.")
                time.sleep(retry_after)
    return all_tweets
