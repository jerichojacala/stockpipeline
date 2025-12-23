from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os
import csv

# =========================
# Configuration
# =========================
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")
ETF = "FTEC"
TOP_N = 10
OUTPUT_DIR = "/opt/airflow/data"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================
# Task 1: Fetch ETF holdings
# =========================
def fetch_holdings(ti):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=ETF_PROFILE&symbol={ETF}&apikey={ALPHA_VANTAGE_KEY}"
    )
    data = requests.get(url).json()

    if not data or "holdings" not in data:
        raise ValueError("No holdings returned from Alpha Vantage")

    #get only top N holdings, should be defined in config section of this file
    df = pd.DataFrame(data["holdings"]).head(TOP_N)

    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")

    # Convert percentage to fraction, should not actually be needed
    if df["weight"].max() > 1:
        df["weight"] = df["weight"] / 100

    #calculate timestamp and write holdings to a new output file
    timestamp = datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
    df.to_csv(f"{OUTPUT_DIR}/{ETF}_holdings_{timestamp}.csv", index=False)

    # turn the dataframe into a list of dicts for XCom
    ti.xcom_push(key="holdings", value=df.to_dict("records"))
    ti.xcom_push(key="timestamp", value=timestamp)

# =========================
# Task 2: Fetch fundamentals
# =========================
def fetch_fundamentals(ti):
    holdings = ti.xcom_pull(task_ids="fetch_holdings", key="holdings")

    results = []
    for h in holdings:
        symbol = h["symbol"]

        url = (
            "https://www.alphavantage.co/query"
            f"?function=OVERVIEW&symbol={symbol}&apikey={ALPHA_VANTAGE_KEY}"
        )
        resp = requests.get(url).json()

        try:
            pe = float(resp.get("PERatio"))
            market_cap = float(resp.get("MarketCapitalization"))
        except (TypeError, ValueError):
            continue

        if pe <= 0 or market_cap <= 0:
            continue

        earnings = market_cap / pe

        results.append({
            "symbol": symbol,
            "weight": h["weight"],
            "market_cap": market_cap,
            "earnings": earnings,
        })

    if not results:
        raise ValueError("No valid fundamentals found")

    # turn the datafram into a list of dicts for XCom
    df = pd.DataFrame(results)
    ti.xcom_push(key="fundamentals", value=df.to_dict("records"))

# =========================
# Task 3: Calculate ETF P/E
# =========================
def calculate_etf_pe(ti):
    rows = ti.xcom_pull(task_ids="fetch_fundamentals", key="fundamentals")
    timestamp = ti.xcom_pull(task_ids="fetch_holdings", key="timestamp")

    df = pd.DataFrame(rows)

    # Normalize weights
    df["weight"] = df["weight"] / df["weight"].sum()

    weighted_market_cap = (df["weight"] * df["market_cap"]).sum()
    weighted_earnings = (df["weight"] * df["earnings"]).sum()

    etf_pe = weighted_market_cap / weighted_earnings

    result_path = f"{OUTPUT_DIR}/etf_pe.csv"
    write_header = not os.path.exists(result_path)

    with open(result_path, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "ETF",
                "timestamp",
                "etf_pe",
                "top_n",
                "coverage_weight"
            ])
        writer.writerow([
            ETF,
            timestamp,
            round(etf_pe, 2),
            len(df),
            round(df["weight"].sum(), 4)
        ])

    print(f"{ETF} ETF P/E = {etf_pe:.2f}")

# =========================
# DAG definition
# =========================
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="etf_pe_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="fetch_holdings",
        python_callable=fetch_holdings,
    )

    t2 = PythonOperator(
        task_id="fetch_fundamentals",
        python_callable=fetch_fundamentals,
    )

    t3 = PythonOperator(
        task_id="calculate_etf_pe",
        python_callable=calculate_etf_pe,
    )

    t1 >> t2 >> t3
