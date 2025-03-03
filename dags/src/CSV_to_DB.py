# CSV_to_DB.py - Upload CSV data to PostgreSQL
import os
from sqlalchemy import text
import subprocess
from src.db_connection import *

# Function to create table for user reviews
# Function to create table for user reviews
def create_table_user_review():
    """Creates the user_reviews table in PostgreSQL."""
    engine = connect_with_db()
    with engine.begin() as connection:
        try:
            query = text("""
                CREATE TABLE IF NOT EXISTS user_reviews (
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
            print("User Reviews table created successfully.")
        except Exception as e:
            print(f"Error creating user_reviews table: {e}")

# Function to create table for metadata
def create_table_meta_data():
    """Creates the metadata table in PostgreSQL."""
    engine = connect_with_db()
    with engine.begin() as connection:
        try:
            query = text("""
                CREATE TABLE IF NOT EXISTS metadata (
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
            print("Metadata table created successfully.")
        except Exception as e:
            print(f"Error creating metadata table: {e}")

# Function to load metadata CSV into PostgreSQL
def add_meta_data():
    """Uploads metadata CSV from local machine to PostgreSQL inside Docker."""
    
    # Define CSV file path
    csv_path = "Data/CSV/TEST/test_metadata.csv"
    
    # Ensure the file exists before proceeding
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found!")
        return
    
    # Copy CSV file into the PostgreSQL container
    try:
        subprocess.run(["docker", "cp", csv_path, "ml_ops-postgres-1:/tmp/test_metadata.csv"], check=True)
        print("CSV file copied to PostgreSQL container.")
    except subprocess.CalledProcessError as e:
        print(f"Error copying file: {e}")
        return
    
    # Run the `\copy` command inside PostgreSQL
    transfer_command = """
    docker exec -i ml_ops-postgres-1 psql -U airflow -d airflow -c "
    \\copy metadata FROM '/tmp/test_metadata.csv' DELIMITER ',' CSV HEADER;
    "
    """
    
    try:
        subprocess.run(transfer_command, shell=True, check=True)
        print("Metadata successfully inserted into PostgreSQL!")
    except subprocess.CalledProcessError as e:
        print(f"Error during import: {e}")


# Function to load review CSV into PostgreSQL
import os
import subprocess

def add_review_data():
    """Uploads user review data from CSV to PostgreSQL inside Docker."""
    
    # Define CSV file path
    csv_path = "Data/CSV/TEST/test_user_review.csv"
    
    # Ensure the file exists before proceeding
    if not os.path.exists(csv_path):
        print(f"Error: CSV file {csv_path} not found!")
        return
    
    # Copy CSV file into the PostgreSQL container
    try:
        subprocess.run(["docker", "cp", csv_path, "ml_ops-postgres-1:/tmp/test_user_review.csv"], check=True)
        print("CSV file copied to PostgreSQL container.")
    except subprocess.CalledProcessError as e:
        print(f"Error copying file: {e}")
        return
    
    # Run the `\copy` command inside PostgreSQL
    transfer_command = """
    docker exec -i ml_ops-postgres-1 psql -U airflow -d airflow -c "
    \\copy user_reviews FROM '/tmp/test_user_review.csv' DELIMITER ',' CSV HEADER;
    "
    """
    
    try:
        subprocess.run(transfer_command, shell=True, check=True)
        print("Data successfully inserted into PostgreSQL!")
    except subprocess.CalledProcessError as e:
        print(f"Error during import: {e}")
