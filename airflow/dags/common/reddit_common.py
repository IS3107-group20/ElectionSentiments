import re
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
    
def clean_and_lemmatize(text, lemmatizer, stop_words):
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

def classify_sentiment(score):
    if score >= 0.05:
        return 'Positive'
    elif score <= -0.05:
        return 'Negative'
    else:
        return 'Neutral'
