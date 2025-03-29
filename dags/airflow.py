from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.db_connection import *
from src.json_to_csv import json_to_csv_meta, json_to_csv_review
from src.CSV_to_DB import create_table_user_review, create_table_meta_data, add_meta_data, add_review_data
from src.db_to_schema import db_to_schema
from src.download_data import ingest_data_meta, ingest_data_review

default_args = {
    'owner': 'ML_OPS',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=0.3),
}

dag = DAG(
    'amazon_reviews_pipeline',
    default_args=default_args,
    description='DAG for processing Amazon Automotive reviews and metadata',
    schedule_interval=None,
    catchup=False,
)

# Task: Download metadata ZIP and upload its content to GCS
get_metadata = PythonOperator(
    task_id='get_metadata',
    python_callable=ingest_data_meta,
    op_args=['https://raw.githubusercontent.com/Susilkessav/ML_OPS/main/test_metadata.zip'],
    dag=dag,
)

# Task: Download review data ZIP and upload its content to GCS
get_review_data = PythonOperator(
    task_id='get_review_data',
    python_callable=ingest_data_review,
    op_args=['https://raw.githubusercontent.com/Susilkessav/ML_OPS/main/test_user_review.zip'],
    dag=dag,
)

# Task: Convert review JSON to CSV and upload CSV to GCS
json_to_csv_review_data = PythonOperator(
    task_id='json_to_csv_review_data',
    python_callable=json_to_csv_review,
    op_args=['gs://mlops_data_pipeline/Data/Raw/TEST/test_user_review.json', 'Data/Processed/Reviews/'],
    dag=dag,
)

# Task: Convert metadata JSON to CSV and upload CSV to GCS
json_to_csv_meta_data = PythonOperator(
    task_id='json_to_csv_meta_data',
    python_callable=json_to_csv_meta,
    op_args=['gs://mlops_data_pipeline/Data/Raw/TEST/test_metadata.json','Data/Processed/Metadata/'],
    dag=dag,
)

# Task: Create raw tables for review data and metadata
createtableuserreview = PythonOperator(
    task_id='createtableuserreview',
    python_callable=create_table_user_review,
    dag=dag,
)

createtablemetadata = PythonOperator(
    task_id='createtablemetadata',
    python_callable=create_table_meta_data,
    dag=dag,
)

# Task: Transfer CSV data into raw tables in PostgreSQL
transfer_task_meta = PythonOperator(
    task_id='transfer_task_meta',
    python_callable=add_meta_data,
    dag=dag,
)

transfer_task_review = PythonOperator(
    task_id='transfer_task_review',
    python_callable=add_review_data,
    dag=dag,
)

# Task: Migrate data from raw tables to structured schema
dbtoschema = PythonOperator(
    task_id='dbtoschema',
    python_callable=db_to_schema,
    dag=dag,
)

# Define task dependencies
parallel_tasks_1 = [get_review_data, get_metadata]
parallel_tasks_2 = [json_to_csv_review_data, json_to_csv_meta_data, createtableuserreview, createtablemetadata]    
parallel_tasks_3 = [transfer_task_review, transfer_task_meta]

for task in parallel_tasks_1:
    task >> parallel_tasks_2

for task in parallel_tasks_2:
    task >> parallel_tasks_3

for task in parallel_tasks_3:
    task >> dbtoschema
