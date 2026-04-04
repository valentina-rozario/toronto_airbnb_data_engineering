"""## Start exporting files to AWS"""

import boto3
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone


def get_s3_client():
    #load variables locally
    load_dotenv()
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )


def stream_data_to_s3(s3_client):
    bucket_name = os.getenv('S3_BUCKET_NAME')
    #set S3 folder name to current date
    s3_folder_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Define your Data Map - use a dictionary : { "S3_Path": "Inside_Airbnb_URL" }
    data_to_load = {
        f"raw/toronto/{s3_folder_date}/listings.csv.gz": "https://data.insideairbnb.com/canada/on/toronto/2025-11-11/data/listings.csv.gz",
        f"raw/toronto/{s3_folder_date}/reviews.csv.gz": "https://data.insideairbnb.com/canada/on/toronto/2025-11-11/data/reviews.csv.gz",
        f"raw/toronto/{s3_folder_date}/short-term-rental-registrations-data.csv": "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/2ab20f80-3599-486a-8f8a-9cb59117977c/resource/9c235257-b09f-441e-bcad-1495607f9a82/download/short-term-rental-registrations-data.csv"
    }

    print(f"Starting ingestion into bucket: {bucket_name}")

    # Execution Loop (Pure Streaming)
    for s3_path, url in data_to_load.items():
        try:
            file_name = s3_path.split('/')[-1]
            print(f"Streaming: {file_name}...")

            # Using 'with' ensures the connection closes properly
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                # upload_fileobj streams directly from the web to S3 bucket
                s3_client.upload_fileobj(r.raw, bucket_name, s3_path)

            print(f"Successfully landed in S3: {s3_path}")

        except Exception as e:
            print(f"Failed to stream {file_name}: {e}")
            # raise error so github action stops
            raise e 

    print("\n Ingestion Complete! All files are in S3.")

if __name__ == "__main__":
    # Create the client using our setup function
    s3 = get_s3_client()
    
    try:
        # Run the streaming logic
        stream_data_to_s3(s3)
    except Exception as err:
        print(f"Upload failed due to error: {err}")



