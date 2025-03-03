import os
import pg8000
import sqlalchemy
import logging
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector, IPTypes

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def connect_with_db() -> sqlalchemy.engine.base.Engine:
    """
    Establishes a connection to the PostgreSQL database.
    Supports both Google Cloud SQL and local PostgreSQL instances.
    
    Returns:
        sqlalchemy.engine.base.Engine: SQLAlchemy engine for database operations.
    """

    # Retrieve database credentials and connection details from environment variables
    instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")  # GCP Cloud SQL instance
    db_user = os.getenv("DB_USER", "airflow")  # Default user
    db_pass = os.getenv("DB_PASS", "airflow")  # Default password
    db_name = os.getenv("DB_NAME", "airflow")  # Default DB name
    db_host = os.getenv("DB_HOST", "postgres")  # Default PostgreSQL host (Docker container)
    db_port = os.getenv("DB_PORT", "5432")  # Default PostgreSQL port
    
    use_gcp = os.getenv("USE_GCP_SQL")  # Flag to determine if using Google Cloud SQL

    if use_gcp:
        # If connecting to Google Cloud SQL
        ip_type = IPTypes.PRIVATE if os.getenv("PRIVATE_IP") else IPTypes.PUBLIC
        logging.info(f"Connecting to Cloud SQL instance {instance_connection_name} using {ip_type} IP...")

        # Create Google Cloud SQL connector
        connector = Connector()

        def getconn() -> pg8000.dbapi.Connection:
            try:
                conn: pg8000.dbapi.Connection = connector.connect(
                    instance_connection_name,  # Connection name for the GCP instance
                    "pg8000",  # PostgreSQL driver
                    user=db_user,
                    password=db_pass,
                    db=db_name,
                    ip_type=ip_type,  # Public or private IP
                )
                logging.info("Successfully connected to Google Cloud SQL.")
                return conn
            except Exception as e:
                logging.error(f"Cloud SQL connection failed: {e}")
                raise

        # Create SQLAlchemy engine with the GCP connection
        engine = sqlalchemy.create_engine(
            "postgresql+pg8000://",  
            creator=getconn,
        )

    else:
        # If connecting to local PostgreSQL (Docker or standalone)
        logging.info(f"Connecting to PostgreSQL at {db_host}:{db_port} with user {db_user}...")

        try:
            db_url = f"postgresql+pg8000://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
            engine = sqlalchemy.create_engine(db_url)
            logging.info("Successfully connected to local PostgreSQL.")
        except Exception as e:
            logging.error(f"Local PostgreSQL connection failed: {e}")
            raise

    return engine  # Return the SQLAlchemy engine

if __name__ == "__main__":
    try:
        engine = connect_with_db()
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text("SELECT NOW();"))
            print(f"Connected to DB! Current time: {result.scalar()}")
    except Exception as e:
        print(f"Database connection failed: {e}")