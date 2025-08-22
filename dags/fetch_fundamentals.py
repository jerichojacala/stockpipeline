# dags/fetch_fundamentals.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os

ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")

def fetch_fundamentals(ti, **context):
    # Pull tickers from XCom of DAG 1
    tickers = ti.xcom_pull(
        dag_id="fetch_etf_holdings",
        task_ids="fetch_holdings",
        key="tickers"
    )
    if not tickers:
        raise ValueError("No tickers found in XCom")

    timestamp = ti.xcom_pull(
        dag_id="fetch_etf_holdings",
        task_ids="fetch_holdings",
        key="time"
    )

    results = []
    for ticker in tickers:
        url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={ALPHA_VANTAGE_KEY}"
        resp = requests.get(url).json()
        fundamentals = {
            "symbol": ticker,
            "pe_ratio": resp.get("PERatio"),
            "market_cap": resp.get("MarketCapitalization"),
            "fetched_at": datetime.utcnow(),
        }
        results.append(fundamentals)

    pd.DataFrame(results).to_csv(f"/opt/airflow/data/fundamentals_{timestamp}.csv", index=False)

with DAG(
    dag_id="fetch_fundamentals",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # only triggered by DAG 1
    catchup=False,
) as dag:
    PythonOperator(
        task_id="fetch_fundamentals_task",
        python_callable=fetch_fundamentals,
    )
