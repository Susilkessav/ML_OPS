import os
import logging
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_gcs_client():
    """
    Initialize a GCS client and return it.
    """
    try:
        client = storage.Client()
        return client
    except Exception as e:
        logging.error(f"Error initializing GCS client: {e}")
        return None

def connect_to_bucket():
    """
    Connects to a Google Cloud Storage bucket.
    """
    bucket_name = os.getenv("GCS_BUCKET_NAME")  # Fetch bucket name from .env
    client = get_gcs_client()
    
    if client is None:
        logging.error("Unable to establish a GCS client connection.")
        return None
    
    try:
        bucket = client.bucket(bucket_name)
        logging.info(f"Successfully connected to bucket: {bucket_name}")
        return bucket
    except Exception as e:
        logging.error(f" Error connecting to bucket {bucket_name}: {e}")
        return None

def upload_blob(source_file_name: str, destination_blob_name: str):
    """
    Uploads a file to Google Cloud Storage.

    Args:
        source_file_name (str): Local file path.
        destination_blob_name (str): Destination path in GCS.

    Returns:
        None
    """
    bucket = connect_to_bucket()
    if bucket is None:
        logging.error("Bucket connection failed. Cannot upload file.")
        return
    
    try:
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        logging.info(f" Successfully uploaded {source_file_name} to {destination_blob_name}.")
    except Exception as e:
        logging.error(f"Error uploading {source_file_name} to GCS: {e}")

def download_blob(source_blob_name: str, destination_file_name: str):
    """Downloads a file from Google Cloud Storage."""
    bucket = connect_to_bucket()
    if bucket is None:
        logging.error("Bucket connection failed. Cannot download file.")
        return
    
    try:
        # Ensure we are not prepending 'gs://' twice
        if source_blob_name.startswith("gs://"):
            source_blob_name = source_blob_name.replace("gs://mlops_data_pipeline/", "")

        logging.info(f"Downloading from GCS: {source_blob_name} to local path: {destination_file_name}")
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        logging.info(f"Successfully downloaded {source_blob_name} to {destination_file_name}.")
    except Exception as e:
        logging.error(f"Error downloading {source_blob_name}: {e}")


def list_blobs():
    """
    Lists all files (blobs) in the connected GCS bucket.

    Returns:
        None
    """
    bucket = connect_to_bucket()
    if bucket is None:
        logging.error("Bucket connection failed. Cannot list files.")
        return
    
    try:
        blobs = list(bucket.list_blobs())
        logging.info("Files in the bucket:")
        for blob in blobs:
            print(f" - {blob.name}")
    except Exception as e:
        logging.error(f"Error listing blobs: {e}")
