from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import pandas as pd
import os

# Load from environment in real projects; hardcoded here for clarity
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")
ETF = "FTEC"

# Where we want to store data locally
OUTPUT_DIR = "/opt/airflow/data"  # this directory should exist in your docker volume
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_holdings(ti, **context):
    url = f"https://www.alphavantage.co/query?function=ETF_PROFILE&symbol={ETF}&apikey={ALPHA_VANTAGE_KEY}"
    response = requests.get(url)

    if "holdings" not in response:
        raise ValueError(f"Unexpected response: {response}")

    data = response.json()
    num_stocks = 10
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

    ti.xcom_push(key="tickers", value=holdings_df["symbol"].head(num_stocks).tolist())
    ti.xcom_push(key="weights", value=holdings_df["weight"].head(num_stocks).tolist())
    ti.xcom_push(key="time", value=timestamp)

def fetch_fundamentals(ti, **context):
    # Pull tickers from XCom of DAG 1
    tickers = ti.xcom_pull(
        task_ids="fetch_holdings",
        key="tickers"
    )
    if not tickers:
        raise ValueError("No tickers found in XCom")

    timestamp = ti.xcom_pull(
        task_ids="fetch_holdings",
        key="time"
    )

    if not timestamp:
        raise ValueError("No timestamp found in XCom")

    results = []
    for ticker in tickers:
        url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={ALPHA_VANTAGE_KEY}"
        resp = requests.get(url).json()
        if resp is None:
            raise ValueError("No response returned while fetching a fundamental for a holding. Check if rate limit was reached.")
        fundamentals = {
            "symbol": ticker,
            "pe_ratio": resp.get("PERatio"),
            "market_cap": resp.get("MarketCapitalization"),
            "fetched_at": datetime.utcnow(),
        }
        results.append(fundamentals)

    pd.DataFrame(results).to_csv(f"/opt/airflow/data/fundamentals_{timestamp}.csv", index=False)

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

    fetch_holdings_task = PythonOperator(
        task_id="fetch_holdings",
        python_callable=fetch_holdings,
        provide_context=True,
    )

    fetch_fundamentals_task = PythonOperator(
        task_id="fetch_fundamentals",
        python_callable=fetch_fundamentals,
        provide_context=True,
    )

    fetch_holdings_task >> fetch_fundamentals_task