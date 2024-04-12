from datetime import datetime, timedelta
from pynytimes import NYTAPI
import pandas as pd
import hashlib
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from google.cloud import bigquery
import os
import pandas as pd
import io
from dotenv import load_dotenv


def get_credentials():
    # Load gbq
    curr_directory = os.getcwd()
    parent_directory = os.path.dirname(curr_directory)
    cred = parent_directory + "/privatekey.json"
    credentials = service_account.Credentials.from_service_account_file(f"{cred}")
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    return client, credentials


def table_exists(dataset_id, table_id):
    client, credentials = get_credentials()
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        # Try to get the table metadata
        client.get_table(table_ref)
        return True  # Table exists
    except Exception as e:
        if "Not found" in str(e):
            return False  # Table does not exist
        else:
            raise e


def calculate_row_checksum(row):
    """
    Calculate a checksum for the given row.
    """
    row_str = row.to_string(index=False)
    checksum = hashlib.md5(row_str.encode()).hexdigest()
    return checksum


def get_current_checksum_list(dataset_id, table_id):
    client, credentials = get_credentials()

    query = f"""SELECT checksum FROM ElectionSentiments.{dataset_id}.{table_id}"""

    print("===========> Getting current checksums in database")
    query_job = client.query(query)
    results = query_job.result()
    current_checksum_ls = []
    for row in results:
        current_checksum_ls.append(row["checksum"])

    return current_checksum_ls


def create_table(**kwargs):

    dataset_id = "ElectionSentiments"
    table_id = "news"
    kwargs["ti"].xcom_push(key="dataset_id", value=dataset_id)
    kwargs["ti"].xcom_push(key="table_id", value=table_id)

    client, credentials = get_credentials()

    # Define the schema for the table
    if table_exists(dataset_id, table_id):
        print(f"=============> Table {dataset_id}.{table_id} already exists")
    else:
        schema = [
            bigquery.SchemaField("headline", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("author", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("lead_paragraph", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("snippet", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("publication_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("keywords", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "checksum", "STRING", mode="NULLABLE", description="Primary Key"
            ),
        ]

        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"=============> Table {table_id} created in dataset {dataset_id}.")


def get_nyt_articles_weekly(query):

    load_dotenv()
    apikey = "jp8j36ahTYcAE3AskMXyvOH0Gx9nrmch"
    nytapi = NYTAPI(apikey, parse_dates=True)

    # Calculate the start date (one week ago)
    start_date = datetime.now() - timedelta(days=7)

    articles = nytapi.article_search(
        query=query,
        options={
            "sort": "relevance",
            "sources": [
                "New York Times",
                "AP",
                "Reuters",
                "International Herald Tribune",
            ],
        },
        dates={"begin": start_date, "end": datetime.now()},
        results=10,
    )

    return articles, nytapi


def transform_into_csv(articles):

    # for each of the articles_archive in the list, get the information that is stored in a nested dictionary:
    headline = map(lambda x: x["headline"]["main"], articles)
    author = map(lambda x: x["headline"]["kicker"], articles)
    leadparagraph = map(lambda x: x["lead_paragraph"], articles)
    snippet = map(lambda x: x["snippet"], articles)
    source = map(lambda x: x["source"] if "source" in x.keys() else "na", articles)
    pubdate = map(lambda x: x["pub_date"], articles)

    # since keywords are a branch down in the nested dictionary, we need to add an additional for loop to collect all keywords:
    keywords = map(lambda x: "; ".join(i["value"] for i in x["keywords"]), articles)

    # transforming the data into a pandas dataframe:
    data = {
        "headline": list(headline),
        "author": list(author),
        "lead_paragraph": list(leadparagraph),
        "snippet": list(snippet),
        "source": list(source),
        "publication_date": list(pubdate),
        "keywords": list(keywords),
    }
    df = pd.DataFrame(data)

    return df


def process_news_data(**kwargs):
    _, credentials = get_credentials()

    dataset_id = kwargs["ti"].xcom_pull(key="dataset_id")
    table_id = kwargs["ti"].xcom_pull(key="table_id")

    # Get news data and apply checksum
    articles_trump, _ = get_nyt_articles_weekly("trump")
    articles_biden, _ = get_nyt_articles_weekly("biden")
    news_data_trump = transform_into_csv(articles_trump)
    news_data_biden = transform_into_csv(articles_biden)

    news_data = pd.concat([news_data_biden, news_data_trump])

    news_data["checksum"] = news_data.apply(calculate_row_checksum, axis=1)
    current_checksum_ls = get_current_checksum_list(dataset_id, table_id)

    # Filter out new rows
    new_news_data = news_data[~news_data["checksum"].isin(current_checksum_ls)]

    destTable = dataset_id + "." + table_id
    numrows = len(new_news_data)

    if numrows == 0:
        print("===========> Data is up to date, no new data added")
    else:
        new_news_data.to_gbq(
            destination_table=destTable,
            project_id=credentials.project_id,
            credentials=credentials,
            if_exists="append",
        )
        print(
            f"============> Successfully inserted {numrows} rows into {destTable} table"
        )


with DAG(
    "news_dag",
    default_args={
        "depends_on_past": False,
        "email": ["tammyypx@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Dag to scrape and retrieve data from news api",
    schedule_interval=timedelta(weeks=1),
    start_date=datetime(2024, 2, 28),
    dagrun_timeout=timedelta(seconds=5),
    catchup=False,
    tags=["news_dag"],
) as dag:

    create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
        dag=dag,
    )

    process_news_data = PythonOperator(
        task_id="process_news_data",
        python_callable=process_news_data,
        dag=dag,
    )

    create_table >> process_news_data
