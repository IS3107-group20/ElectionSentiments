import pandas as pd
import re
import praw
import torch
import torch.nn.functional as F
from datetime import datetime
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk import download

download('vader_lexicon')
download('wordnet')
download('stopwords')

def get_reddit_client():
        return praw.Reddit(client_id='9Vy8b4OZfZhDHn0bD4Q94w', 
                           client_secret='h-JRuPyJJBWrWT8qnrt9PCL2V39RbA', 
                           user_agent='is3107')

def get_reddit_data():
    reddit = get_reddit_client()
    subreddits = ['2024elections', 'JoeBiden', 'trump', '2024Election']
    all_posts = []
    
    for subreddit_name in subreddits:
        subreddit = reddit.subreddit(subreddit_name)
        for post in subreddit.hot(limit=100):  # Adjust limit as needed
            all_posts.append([
                str(post.title), post.score, post.id, str(post.subreddit), post.url,
                post.num_comments, str(post.selftext), datetime.fromtimestamp(post.created).isoformat()
            ])
    
    return pd.DataFrame(all_posts, columns=[
        'title', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created'
    ])

def process_data(df):
    # Convert all textual columns to strings to prevent type issues
    df['title'] = df['title'].astype(str)
    df['body'] = df['body'].astype(str)

    sia = SentimentIntensityAnalyzer()
    lemmatizer = WordNetLemmatizer()
    stop_words = set(stopwords.words('english'))

    # Load ABSA model
    absa_tokenizer = AutoTokenizer.from_pretrained("yangheng/deberta-v3-base-absa-v1.1")
    absa_model = AutoModelForSequenceClassification.from_pretrained("yangheng/deberta-v3-base-absa-v1.1")

    def clean_and_lemmatize(text):
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        text = re.sub(r'@\w+', '', text)
        text = re.sub(r'#', '', text)
        text = re.sub(r'RT[\s]+', '', text)
        text = re.sub(r"[^a-zA-Z\s]", ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        words = text.lower().split()
        return ' '.join([lemmatizer.lemmatize(word) for word in words if word not in stop_words])

    df['cleaned text'] = df.apply(lambda x: clean_and_lemmatize(x['title'] + " " + x['body']), axis=1)
    df['sentiment_score'] = df['cleaned text'].apply(lambda text: sia.polarity_scores(text)['compound'])
    df['sentiment'] = df['sentiment_score'].apply(lambda score: 'Positive' if score >= 0.05 else ('Negative' if score <= -0.05 else 'Neutral'))

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

    df['aspect_sentiments'] = df['cleaned text'].apply(extract_aspects)
    return df

# Define the main function to retrieve and process data
def main():
    df = get_reddit_data()
    processed_data = process_data(df)
    processed_data.to_csv('processed_reddit_data.csv', index=False)
    return processed_data

if __name__ == "__main__":
    df = main()
