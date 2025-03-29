import os
from sqlalchemy import text
import subprocess
from src.db_connection import *

# CSV_to_DB.py (snippet)
from sqlalchemy import text
from src.db_connection import connect_with_db

def create_table_user_review():
    """Creates the raw_user_reviews table in PostgreSQL."""
    engine = connect_with_db()
    with engine.begin() as connection:
        try:
            query = text("""
                CREATE TABLE IF NOT EXISTS raw_user_reviews (
                    rating FLOAT,
                    title TEXT,
                    text TEXT,
                    images TEXT,
                    asin TEXT,
                    parent_asin TEXT,
                    user_id TEXT,
                    timestamp TIMESTAMP,
                    helpful_vote INT,
                    verified_purchase BOOLEAN
                );
            """)
            connection.execute(query)
            print("Raw User Reviews table created successfully.")
        except Exception as e:
            print(f"Error creating raw_user_reviews table: {e}")

def create_table_meta_data():
    """Creates the raw_metadata table in PostgreSQL."""
    engine = connect_with_db()
    with engine.begin() as connection:
        try:
            query = text("""
                CREATE TABLE IF NOT EXISTS raw_metadata (
                    main_category TEXT,
                    title TEXT,
                    average_rating FLOAT,
                    rating_number INT,
                    features TEXT,
                    description TEXT,
                    price TEXT,
                    images TEXT,
                    videos TEXT,
                    store TEXT,
                    categories TEXT,
                    details TEXT,
                    parent_asin TEXT,
                    bought_together TEXT,
                    subtitle TEXT,
                    author TEXT
                );
            """)
            connection.execute(query)
            print("Raw Metadata table created successfully.")
        except Exception as e:
            print(f"Error creating raw_metadata table: {e}")

# Function to load metadata CSV into PostgreSQL
def add_meta_data():
    """Uploads metadata CSV from local machine to PostgreSQL inside Docker."""
    csv_path = "Data/CSV/TEST/test_metadata.csv"
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found!")
        return
    try:
        subprocess.run(["docker", "cp", csv_path, "postgres:/tmp/test_metadata.csv"], check=True)
        print("CSV file copied to PostgreSQL container.")
    except subprocess.CalledProcessError as e:
        print(f"Error copying file: {e}")
        return

    transfer_command = """
    docker exec -i postgres psql -U airflow -d airflow -c "
    \\copy raw_metadata FROM '/tmp/test_metadata.csv' DELIMITER ',' CSV HEADER;
    "
    """
    try:
        subprocess.run(transfer_command, shell=True, check=True)
        print("Metadata successfully inserted into PostgreSQL!")
    except subprocess.CalledProcessError as e:
        print(f"Error during import: {e}")

# Function to load review CSV into PostgreSQL
def add_review_data():
    """Uploads user review data from CSV to PostgreSQL inside Docker."""
    csv_path = "Data/CSV/TEST/test_user_review.csv"
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found!")
        return
    try:
        subprocess.run(["docker", "cp", csv_path, "postgres:/tmp/test_user_review.csv"], check=True)
        print("CSV file copied to PostgreSQL container.")
    except subprocess.CalledProcessError as e:
        print(f"Error copying file: {e}")
        return

    transfer_command = """
    docker exec -i postgres psql -U airflow -d airflow -c "
    \\copy raw_user_reviews FROM '/tmp/test_user_review.csv' DELIMITER ',' CSV HEADER;
    "
    """
    try:
        subprocess.run(transfer_command, shell=True, check=True)
        print("Data successfully inserted into PostgreSQL!")
    except subprocess.CalledProcessError as e:
        print(f"Error during import: {e}")

