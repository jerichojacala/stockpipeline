from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import pandas as pd
import os

# Load from environment in real projects; hardcoded here for clarity
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
ETF = "FTEC"

# Where we want to store data locally
OUTPUT_DIR = "/opt/airflow/data"  # this directory should exist in your docker volume
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_etf_holdings():
    url = f"https://www.alphavantage.co/query?function=ETF_PROFILE&symbol={ETF}&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()

    # Convert holdings into DataFrame
    holdings_df = pd.DataFrame(data["holdings"])
    holdings_df["weight"] = holdings_df["weight"].astype(float)

    # Add timestamp
    timestamp = datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
    filename = f"{ETF}_holdings_{timestamp}.csv"

    filepath = os.path.join(OUTPUT_DIR, filename)
    holdings_df.to_csv(filepath, index=False)
    print(f"Saved {filepath}")

    # push symbols to XCOM to fetch_fundamentals DAG

    ti.xcom_push(key="tickers", value=df["symbol"].tolist())
    ti.xcom_push(key="time", value=timestamp)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="etf_holdings_pipeline",
    default_args=default_args,
    schedule_interval="@daily",  # runs once a day
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_etf_holdings",
        python_callable=fetch_etf_holdings,
    )

    trigger_fundamentals = TriggerDagRunOperator(
        task_id="trigger_fundamentals_dag",
        trigger_dag_id="fetch_fundamentals",
        conf={"from_dag": "fetch_etf_holdings"},
    )

    fetch_task >> trigger_fundamentals