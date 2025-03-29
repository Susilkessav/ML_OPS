# download_data.py - Download and Upload Data
import os
import requests
import zipfile
import shutil
from src.bucket_connection import upload_blob

DATA_DIR = "Data"  # Root directory for storing temporary files

def download_and_extract(file_url, temp_folder):
    """Downloads and extracts a ZIP file from a given URL."""
    filename = os.path.basename(file_url)  # Extract file name from URL
    extract_to = os.path.join(DATA_DIR, temp_folder)  # Extraction directory

    # Ensure the temporary directory exists
    os.makedirs(extract_to, exist_ok=True)

    zipfile_path = os.path.join(extract_to, filename)  # Path to store ZIP file

    print(f"Downloading {filename}...")
    response = requests.get(file_url, timeout=30)
    if response.status_code == 200:
        with open(zipfile_path, "wb") as file:
            file.write(response.content)
    else:
        raise Exception(f"Failed to download {filename}. HTTP Status: {response.status_code}")

    print(f"Extracting {filename}...")
    try:
        with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            extracted_files = zip_ref.namelist()
    except zipfile.BadZipFile:
        os.remove(zipfile_path)
        raise Exception(f"Failed to unzip {zipfile_path}. ZIP file might be corrupted.")

    if not extracted_files:
        raise Exception(f"No files extracted from {filename}.")

    return os.path.join(extract_to, extracted_files[0])

def ingest_data(file_url, temp_folder, cloud_directory):
    """Handles data ingestion: downloading, extracting, and uploading to Google Cloud Storage."""
    try:
        extracted_file = download_and_extract(file_url, temp_folder)
        destination_blob_name = os.path.join(cloud_directory, os.path.basename(extracted_file))
        print(f"Uploading {extracted_file} to {destination_blob_name} in cloud storage...")
        upload_blob(extracted_file, destination_blob_name)
        print("Upload complete.")
        shutil.rmtree(os.path.join(DATA_DIR, temp_folder))
        os.makedirs(os.path.join(DATA_DIR, temp_folder))
        return "Ingestion Completed Successfully."
    except Exception as e:
        print(f"Error during ingestion: {e}")
        return "Ingestion Failed."

def ingest_data_meta(file_url):
    """Downloads, extracts, and uploads metadata files."""
    return ingest_data(file_url, "temp1", "Data/Raw/Metadata/")

def ingest_data_review(file_url):
    """Downloads, extracts, and uploads review data files."""
    return ingest_data(file_url, "temp2", "Data/Raw/Reviews/")
