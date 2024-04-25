import json
import hashlib
from datetime import datetime
import re
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def calculate_row_checksum(row):
    """Calculate a checksum for a row to identify unique articles"""
    row_str = json.dumps(row, default=json_serial, sort_keys=True)
    checksum = hashlib.md5(row_str.encode()).hexdigest()
    return checksum


def classify_topic(keywords_str):
    # Normalize the string by converting to lowercase and splitting on semicolon
    # and strip spaces to ensure consistent keyword matching.
    keywords_list = [keyword.strip().lower() for keyword in keywords_str.split(';')]
    
    trump_keywords = {
        'trump', 'donald', 'donald trump', 'president trump', 'trump administration',
        'trump campaign', 'trump era', 'ivanka', 'melania', 'trump policies', 'maga',
        'make america great again', 'trump supporter', 'trump rally', 'trump impeachment'
    }
    biden_keywords = {
        'biden', 'joe', 'joe biden', 'president biden', 'biden administration',
        'biden campaign', 'biden era', 'hunter biden', 'jill biden', 'biden policies',
        'build back better', 'biden supporter', 'biden rally', 'biden impeachment'
    }

    # Simplified keyword matching for broader matches
    trump_count = sum(any(tk in keyword for tk in trump_keywords) for keyword in keywords_list)
    biden_count = sum(any(bk in keyword for bk in biden_keywords) for keyword in keywords_list)

    if trump_count > biden_count:
        return 'Trump'
    elif biden_count > trump_count:
        return 'Biden'
    return 'Both' if trump_count > 0 and biden_count > 0 else 'None'

def clean_text(text):
    """Clean text by removing URLs, special characters, and normalizing whitespace"""
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#', '', text)
    text = re.sub(r'RT[\s]+', '', text)
    text = re.sub(r"[^a-zA-Z\s]", ' ', text)
    text = re.sub(r'\s+', ' ', text).strip().lower()
    return text

def classify_sentiment(score):
        if score >= 0.05:
            return 'Positive'
        elif score <= -0.05:
            return 'Negative'
        else:
            return 'Neutral'