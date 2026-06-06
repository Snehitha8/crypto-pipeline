import json
import os
from datetime import datetime
from google.cloud import bigquery

# Tell Python where your credentials are
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\snehi\crypto-pipeline\credentials.json"

# Your Google Cloud details
PROJECT_ID = "crypto-pipeline-498202"
DATASET_ID = "crypto_raw"
TABLE_ID = "raw_prices"

def load_to_bigquery(filename):
    # Read the local JSON file
    with open(filename, "r") as f:
        data = json.load(f)
    
    # Add extraction timestamp to each row
    for row in data:
        row["extracted_at"] = datetime.now().isoformat()
    
    # Connect to BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Delete today's data first (idempotency!)
    delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE DATE(extracted_at) = CURRENT_DATE()
    """
    
    try:
        client.query(delete_query).result()
        print("Deleted existing data for today")
    except Exception:
        print("Table doesn't exist yet, will create it")
    
    # Load fresh data
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )
    
    job = client.load_table_from_json(data, table_ref, job_config=job_config)
    job.result()
    
    print(f"Loaded {len(data)} rows into {table_ref}")

if __name__ == "__main__":
    # Find the most recent raw file
    import glob
    files = glob.glob("raw_crypto_*.json")
    latest_file = max(files)
    print(f"Loading file: {latest_file}")
    load_to_bigquery(latest_file)