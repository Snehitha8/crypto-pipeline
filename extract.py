import requests
import json
import os
from datetime import datetime
from google.cloud import storage

# Tell Python where your credentials are
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\snehi\crypto-pipeline\credentials.json"

# Your Google Cloud details
PROJECT_ID = "crypto-pipeline-498202"
BUCKET_NAME = "crypto-raw-data-snehi"  # we'll create this next

def extract_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": False
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    print(f"Extracted {len(data)} coins")
    print(f"First coin: {data[0]['name']} - ${data[0]['current_price']}")
    
    return data

def save_raw_data(data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"raw_crypto_{timestamp}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"Raw data saved locally to {filename}")
    return filename

def upload_to_gcs(filename):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw/{filename}")
    blob.upload_from_filename(filename)
    print(f"Uploaded {filename} to GCS bucket {BUCKET_NAME}")

if __name__ == "__main__":
    data = extract_crypto_data()
    filename = save_raw_data(data)
    upload_to_gcs(filename)