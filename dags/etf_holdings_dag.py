from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import pandas as pd
import os
import csv

# Load from environment in real projects; hardcoded here for clarity
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")
ETF = "FTEC"

# Where we want to store data locally
OUTPUT_DIR = "/opt/airflow/data"  # this directory should exist in your docker volume
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_holdings(ti, **context):
    url = f"https://www.alphavantage.co/query?function=ETF_PROFILE&symbol={ETF}&apikey={ALPHA_VANTAGE_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        raise ValueError(f"Request failed. Status={response.status_code}, Body={response.text[:200]}")

    try:
        data = response.json()
    except Exception as e:
        raise ValueError(f"Failed to parse JSON. Body={response.text[:200]}") from e

    # Check expected structure
    if not data or "holdings" not in data:
        raise ValueError(f"No 'holdings' field found. Response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
    
    num_stocks = 0
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
    fundamentals_df = pd.DataFrame(results)
    ti.xcom_push(key="pe_ratios", value=fundamentals_df["pe_ratio"].tolist())

def calculate_weighted_pe(ti, z = 1.959963984540054, **context):
    pe_ratios = ti.xcom_pull(task_ids="fetch_fundamentals", key="pe_ratios")
    timestamp = ti.xcom_pull(task_ids="fetch_holdings", key="time")
    tickers = ti.xcom_pull(
        task_ids="fetch_holdings",
        key="tickers"
    )
    weights = ti.xcom_pull(task_ids="fetch_holdings", key="weights")

    # change this
    data = {'ticker': tickers, 'weight': weights, 'pe_ratio': pe_ratios}
    df = pd.DataFrame(data)

    # Ensure numeric types
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
    df["pe_ratio"] = pd.to_numeric(df["pe_ratio"], errors="coerce")

    # Drop rows with missing values
    df = df.dropna(subset=["weight", "pe_ratio"])

    if df.empty:
        raise ValueError("No valid data to calculate weighted P/E")
    
    total_weight = df["weight"].sum()

    df["weighted_pe_component"] = df["pe_ratio"] * (df["weight"] / total_weight)
    weighted_pe = df["weighted_pe_component"].sum()
    
    #estimate standard deviation of top 10 stocks
    std_top10 = df["pe_ratio"].std()

    #calculate k as 2 - total weight
    k = 2-total_weight

    #inflate standard deviation for true estimation
    cv = std_top10 / weighted_pe
    sigma_tail = k * cv * weighted_pe

    #calculate standard deviation for tail in relation to whole etf
    sigma_etf = (1 - total_weight) * sigma_tail

    #calculate lower and upper bounds
    lower = weighted_pe - z * sigma_etf
    upper = weighted_pe + z * sigma_etf

    result_path = os.path.join(OUTPUT_DIR, "weighted_pe.csv")
    #pd.DataFrame([{"ETF": ETF, "timestamp": timestamp, "weighted_pe": weighted_pe}]).to_csv(result_path, index=False)
    new_row = [ETF, timestamp, weighted_pe, total_weight, lower, upper, z]

    #write weighted p/e to running file
    with open(result_path, 'a', newline='') as csvfile:
        # Create a CSV writer object
        writer = csv.writer(csvfile)

        # Write the new row to the file
        writer.writerow(new_row)

    print(f"Calculated weighted P/E: {weighted_pe}, saved to {result_path}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="etf_holdings_pipeline",
    default_args=default_args,
    schedule_interval="@monthly",  # runs once a day
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

    calculate_weighted_pe_task = PythonOperator(
        task_id="calculate_weighted_pe",
        python_callable=calculate_weighted_pe,
        provide_context=True,
    )

    fetch_holdings_task >> fetch_fundamentals_task >> calculate_weighted_pe_task