import os
import pg8000
import sqlalchemy
import logging
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def connect_with_db() -> sqlalchemy.engine.base.Engine:
    """
    Connect to PostgreSQL. If USE_GCP_SQL is True, connect to Cloud SQL.
    Otherwise, connect to local Docker Postgres.
    """
    db_user = os.getenv("DB_USER", "airflow")
    db_pass = os.getenv("DB_PASS", "airflow")
    db_name = os.getenv("DB_NAME", "airflow")
    db_host = os.getenv("DB_HOST", "postgres")
    db_port = os.getenv("DB_PORT", "5432")

    use_gcp = os.getenv("USE_GCP_SQL", "False").strip().lower() == "true"

    if use_gcp:
        logging.info("Attempting to connect to Cloud SQL. (USE_GCP_SQL=True)")
        from google.cloud.sql.connector import Connector, IPTypes
        instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
        ip_type = IPTypes.PRIVATE if os.getenv("PRIVATE_IP") else IPTypes.PUBLIC
        connector = Connector()

        def getconn() -> pg8000.dbapi.Connection:
            return connector.connect(
                instance_connection_name,
                "pg8000",
                user=db_user,
                password=db_pass,
                db=db_name,
                ip_type=ip_type,
            )

        engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)
        logging.info("Successfully connected to Cloud SQL.")
    else:
        logging.info(f"Connecting to local Postgres at {db_host}:{db_port}, DB={db_name} ...")
        try:
            db_url = f"postgresql+pg8000://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
            engine = sqlalchemy.create_engine(db_url)
            logging.info("Successfully connected to local PostgreSQL.")
        except Exception as e:
            logging.error(f"Local PostgreSQL connection failed: {e}")
            raise

    return engine

if __name__ == "__main__":
    engine = connect_with_db()
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text("SELECT NOW()"))
        print(f"Connected to DB! Current time: {result.scalar()}")
