import os
import pandas as pd
from dotenv import load_dotenv
from src.bucket_connection import download_blob, upload_blob

load_dotenv()

def json_to_csv(source_blob_name, temp_folder, destination_blob_directory):
    """
    Downloads a JSON file from GCS, converts it to CSV,
    and uploads the CSV back to GCS.
    """
    try:
        file_name = os.path.splitext(os.path.basename(source_blob_name))[0]
        temp_json_file_path = f'Data/{temp_folder}/{file_name}.json'
        temp_csv_file_path = f'Data/{temp_folder}/{file_name}.csv'

        # Ensure the temporary folder exists
        os.makedirs(f'Data/{temp_folder}', exist_ok=True)

        # Download JSON file from GCS
        download_blob(source_blob_name, temp_json_file_path)

        print(f"Converting {file_name}.json to CSV...")
        data = pd.read_json(temp_json_file_path)

        # Save CSV locally
        os.makedirs(os.path.dirname(temp_csv_file_path), exist_ok=True)
        data.to_csv(temp_csv_file_path, index=False)
        print(f"CSV file created: {temp_csv_file_path}")

        # Upload CSV back to GCS
        destination_blob_name = os.path.join(destination_blob_directory, os.path.basename(temp_csv_file_path))
        upload_blob(temp_csv_file_path, destination_blob_name)
        print(f"CSV file uploaded to: {destination_blob_name}")

        return f"Successfully converted {source_blob_name} to {temp_csv_file_path} and uploaded to {destination_blob_name}"
    except Exception as e:
        print(f"Error during conversion: {e}")
        return f"Conversion failed for {source_blob_name}"

# Wrapper functions updated to accept both source blob and destination directory
# def json_to_csv_meta(source_blob_name, destination_blob_directory):
#     return json_to_csv(source_blob_name, "temp", destination_blob_directory)

# def json_to_csv_review(source_blob_name, destination_blob_directory):
#     return json_to_csv(source_blob_name, "temp", destination_blob_directory)
def json_to_csv_meta(source_blob_name, destination_blob_directory):
    return json_to_csv(source_blob_name, "CSV/TEST", destination_blob_directory)

def json_to_csv_review(source_blob_name, destination_blob_directory):
    return json_to_csv(source_blob_name, "CSV/TEST", destination_blob_directory)
