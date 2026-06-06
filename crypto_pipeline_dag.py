from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

default_args = {
    'owner': 'snehi',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_load():
    os.chdir(r'C:\Users\snehi\crypto-pipeline')
    
    # Run extract
    import requests
    import json
    import glob
    from datetime import datetime
    from google.cloud import storage, bigquery
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\snehi\crypto-pipeline\credentials.json"
    
    # Extract
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
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"raw_crypto_{timestamp}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"Extracted {len(data)} coins")
    return filename

def load_to_bq(**context):
    import json
    import glob
    import os
    from datetime import datetime
    from google.cloud import bigquery
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\snehi\crypto-pipeline\credentials.json"
    os.chdir(r'C:\Users\snehi\crypto-pipeline')
    
    files = glob.glob("raw_crypto_*.json")
    latest_file = max(files)
    
    with open(latest_file, "r") as f:
        data = json.load(f)
    
    for row in data:
        row["extracted_at"] = datetime.now().isoformat()
    
    client = bigquery.Client(project="crypto-pipeline-498202")
    table_ref = "crypto-pipeline-498202.crypto_raw.raw_prices"
    
    try:
        client.query(f"DELETE FROM `{table_ref}` WHERE DATE(extracted_at) = CURRENT_DATE()").result()
    except Exception:
        pass
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )
    
    job = client.load_table_from_json(data, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {len(data)} rows into BigQuery")

with DAG(
    dag_id="crypto_pipeline",
    default_args=default_args,
    description="Daily crypto price pipeline",
    schedule="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "pipeline"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_crypto_data",
        python_callable=extract_and_load
    )

    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bq
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd C:/Users/snehi/crypto-pipeline/crypto_transforms && dbt run"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd C:/Users/snehi/crypto-pipeline/crypto_transforms && dbt test"
    )

    extract_task >> load_task >> dbt_run >> dbt_test