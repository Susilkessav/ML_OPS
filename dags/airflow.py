# airflow.py - Main DAG for Amazon Reviews Pipeline
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.db_connection import *
from src.json_to_csv import *
from src.CSV_to_DB import *
from src.db_to_schema import *
from src.download_data import *

# Default arguments for Airflow DAG
default_args = {
    'owner': 'ML_OPS',
    'start_date': datetime(2025, 1, 1),  # Define the DAG start date
    'retries': 2,  # Number of retries on failure
    'retry_delay': timedelta(minutes=0.3),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'amazon_reviews_pipeline',
    default_args=default_args,
    description='DAG for processing Amazon Automotive reviews and metadata',
    schedule_interval=None,  # Manually triggered DAG
    catchup=False,
)

# Task: Download metadata
get_metadata = PythonOperator(
    task_id='get_metadata',
    python_callable=ingest_data_meta,
    op_args=['https://raw.githubusercontent.com/Susilkessav/ML_OPS/main/test_metadata.zip'],
    dag=dag,
)

# Task: Download review data
get_review_data = PythonOperator(
    task_id='get_review_data',
    python_callable=ingest_data_review,
    op_args=['https://raw.githubusercontent.com/Susilkessav/ML_OPS/main/test_user_review.zip'],
    dag=dag,
)

# Task: Convert review JSON to CSV
json_to_csv_review_data = PythonOperator(
    task_id='json_to_csv_review_data',
    python_callable=json_to_csv_review,
    op_args=['gs://mlops_data_pipeline/Data/Raw/TEST/test_user_review.json', 'Data/CSV/TEST/'],
    dag=dag,
)

# Task: Convert metadata JSON to CSV
json_to_csv_meta_data = PythonOperator(
    task_id='json_to_csv_meta_data',
    python_callable=json_to_csv_meta,
    op_args=['gs://mlops_data_pipeline/Data/Raw/TEST/test_metadata.json','Data/CSV/TEST/'],
    dag=dag,
)

# Task: Create tables for review data
createtableuserreview = PythonOperator(
    task_id='createtableuserreview',
    python_callable=create_table_user_review,
    dag=dag,
)

# Task: Create tables for metadata
createtablemetadata = PythonOperator(
    task_id='createtablemetadata',
    python_callable=create_table_meta_data,
    dag=dag,
)

# Task: Transfer metadata to database
transfer_task_meta = PythonOperator(
    task_id='transfer_task_meta',
    python_callable=add_meta_data,
    dag=dag,
)

# Task: Transfer review data to database
transfer_task_review = PythonOperator(
    task_id='transfer_task_review',
    python_callable=add_review_data,
    dag=dag,
)

# Task: Finalize schema and structure data
dbtoschema = PythonOperator(
    task_id='dbtoschema',
    python_callable=db_to_schema,
    dag=dag,
)

# Define dependencies between tasks
parallel_tasks_1 = [get_review_data, get_metadata]
parallel_tasks_2 = [json_to_csv_review_data, json_to_csv_meta_data, createtableuserreview, createtablemetadata]    
parallel_tasks_3 = [transfer_task_review, transfer_task_meta]

for task in parallel_tasks_1:
    task >> parallel_tasks_2

for task in parallel_tasks_2:
    task >> parallel_tasks_3

for task in parallel_tasks_3:
    task >> dbtoschema