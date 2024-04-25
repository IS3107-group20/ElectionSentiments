from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import torch.nn.functional as F


def bq_load_data(df, schema, table):
    # Connection and client setup
    bigquery_conn_id = "google_cloud_default"
    dataset_table = table
    hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id, use_legacy_sql=False)
    credentials = hook.get_credentials()
    client = bigquery.Client(credentials=credentials, project=hook.project_id)
    table_id = f"{hook.project_id}.{dataset_table}"

    # Configure the load job to append data
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    # Execute the load operation
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the load job to complete

    print(f"Loaded {job.output_rows} rows into {table_id}")


def extract_aspects(text):
    absa_tokenizer = AutoTokenizer.from_pretrained("yangheng/deberta-v3-base-absa-v1.1")
    absa_model = AutoModelForSequenceClassification.from_pretrained(
        "yangheng/deberta-v3-base-absa-v1.1"
    )
    aspects = ["trump", "biden"]
    aspect_sentiments = {}
    if text.strip():
        for aspect in aspects:
            inputs = absa_tokenizer(
                f"[CLS] {text} [SEP] {aspect} [SEP]", return_tensors="pt"
            )
            outputs = absa_model(**inputs)
            probs = F.softmax(outputs.logits, dim=1)
            probs = probs.detach().numpy()[0]
            aspect_sentiments[aspect] = {
                label: prob
                for prob, label in zip(probs, ["negative", "neutral", "positive"])
            }
    return aspect_sentiments


def get_existing_ids_by_source(name):
    bigquery_conn_id = "google_cloud_default"
    hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id, use_legacy_sql=False)
    client = bigquery.Client(
        credentials=hook.get_credentials(), project=hook.project_id
    )
    if name == "news_scraped":
        query = f"SELECT checksum FROM `is3107-project-419009.reddit.{name}`"
    else:
        query = f"SELECT id FROM `is3107-project-419009.reddit.{name}`"
    query_job = client.query(query)
    results = query_job.result()
    if name == "news_scraped":
        existing_ids = {row.checksum for row in results}
    else:
        existing_ids = {row.id for row in results}
    return existing_ids


def update_topic_sentiment(df):
    def get_max_sentiment(sentiments):
        max_sentiment = max(sentiments, key=sentiments.get)
        max_score = sentiments[max_sentiment]
        return max_sentiment, max_score

    new_rows = []

    for index, row in df.iterrows():
        if row["topic"] == "Both":
            for aspect, sentiments in row["aspect_sentiments"].items():
                max_sentiment, max_score = get_max_sentiment(sentiments)
                new_row = row.copy()
                new_row["topic"] = "Trump" if aspect == "trump" else "Biden"
                new_row["sentiment"] = max_sentiment
                new_row["sentiment_score"] = max_score
                new_rows.append(new_row)
        else:
            new_rows.append(row)
    return new_rows
