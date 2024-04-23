from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG definition
dag = DAG(
    'bigquery_to_posts',
    default_args=default_args,
    description='Load data from BigQuery tables into a consolidated table',
    schedule_interval='@daily',
)

# Define tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag
)

extract_news = BigQueryOperator(
    task_id='extract_news',
    sql='SELECT * FROM `is3107-project-419009.reddit.news_scraped`',
    use_legacy_sql=False,
    destination_dataset_table='is3107-project-419009.reddit.news_scraped_temp',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

extract_reddit = BigQueryOperator(
    task_id='extract_reddit',
    sql='SELECT * FROM `is3107-project-419009.reddit.reddit_scraped`',
    use_legacy_sql=False,
    destination_dataset_table='is3107-project-419009.reddit.reddit_scraped_temp',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

extract_twitter = BigQueryOperator(
    task_id='extract_twitter',
    sql='SELECT * FROM `is3107-project-419009.reddit.twitter_scraped`',
    use_legacy_sql=False,
    destination_dataset_table='is3107-project-419009.reddit.twitter_scraped_temp',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

def transform_data(**kwargs):
    # Implement transformation logic 
    pass

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_to_posts = BigQueryOperator(
    task_id='load_to_posts',
    sql='SELECT * FROM `transformed_data_table`',
    destination_dataset_table='is3107-project-419009.reddit.posts',
    write_disposition='WRITE_APPEND',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag
)

# Set task dependencies
start_task >> [extract_news, extract_reddit, extract_twitter] >> transform_task >> load_to_posts >> end_task
