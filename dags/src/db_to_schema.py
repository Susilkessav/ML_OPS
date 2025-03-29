import os
from sqlalchemy import text
from src.db_connection import connect_with_db

def db_to_schema():
    """Creates structured tables and migrates data from raw tables to structured ones."""
    engine = connect_with_db()
    try:
        with engine.begin() as connection:
            print("Creating structured tables and migrating data...")

            # Create structured tables
            table_queries = [
                """
                CREATE TABLE IF NOT EXISTS product_images (
                    parent_asin TEXT,
                    thumb TEXT,
                    hi_res TEXT,
                    large_res TEXT
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS product_metadata (
                    parent_asin TEXT,
                    title TEXT,
                    average_rating FLOAT,
                    rating_number INT,
                    features TEXT,
                    description TEXT,
                    price TEXT,
                    store TEXT,
                    details TEXT,
                    main_category TEXT
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS product_categories (
                    parent_asin TEXT,
                    categories TEXT
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS user_reviews (
                    rating FLOAT,
                    title TEXT,
                    text TEXT,
                    asin TEXT,
                    parent_asin TEXT,
                    user_id TEXT,
                    timestamp BIGINT,
                    helpful_vote INT,
                    verified_purchase BOOLEAN
                );
                """
            ]
            for query in table_queries:
                connection.execute(text(query))
            print("Structured tables created successfully.")

            # Migrate data from raw tables to structured tables
            migration_queries = [
                """
                INSERT INTO product_images (parent_asin, thumb, hi_res, large_res)
                SELECT parent_asin,
                    COALESCE(images::jsonb -> 0 ->> 'small_image_url', '') AS thumb,
                    COALESCE(images::jsonb -> 0 ->> 'medium_image_url', '') AS hi_res,
                    COALESCE(images::jsonb -> 0 ->> 'large_image_url', '') AS large_res
                FROM raw_metadata;
                """,
                """
                INSERT INTO product_metadata (parent_asin, title, average_rating, rating_number, features, description, price, store, details, main_category)
                SELECT parent_asin, title, average_rating, rating_number, features, description, price, store, details, main_category
                FROM raw_metadata;
                """,
                """
                INSERT INTO product_categories (parent_asin, categories)
                SELECT parent_asin, categories FROM raw_metadata;
                """,
                """
                INSERT INTO user_reviews (rating, title, text, asin, parent_asin, user_id, timestamp, helpful_vote, verified_purchase)
                SELECT rating, title, text, asin, parent_asin, user_id, EXTRACT(EPOCH FROM timestamp)::BIGINT, helpful_vote, verified_purchase 
                FROM raw_user_reviews;
                """
            ]
            for query in migration_queries:
                connection.execute(text(query))
            print("Data migration completed successfully.")
            return "Schema migration successful."
    except Exception as e:
        print(f"Schema migration error: {e}")
        return "Schema migration failed."
