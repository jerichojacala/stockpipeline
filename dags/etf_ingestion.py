import requests
import pandas as pd

# Example: pulling directly from your Alpha Vantage endpoint
# Replace with your actual API key + ticker
API_KEY = "YOUR_ALPHA_VANTAGE_KEY"
ETF = "FTEC"
url = f"https://www.alphavantage.co/query?function=ETF_PROFILE&symbol={ETF}&apikey={API_KEY}"

response = requests.get(url)
data = response.json()

# ---- Parse ETF metadata ----
meta = {
    "net_assets": float(data["net_assets"]),
    "expense_ratio": float(data["net_expense_ratio"]),
    "turnover": float(data["portfolio_turnover"]),
    "dividend_yield": float(data["dividend_yield"]),
    "inception_date": data["inception_date"],
    "leveraged": data["leveraged"]
}
print("ETF Metadata:", meta)

# ---- Parse Sectors into DataFrame ----
sectors_df = pd.DataFrame(data["sectors"])
sectors_df["weight"] = sectors_df["weight"].astype(float)
print("\nSectors:")
print(sectors_df)

# ---- Parse Holdings into DataFrame ----
holdings_df = pd.DataFrame(data["holdings"])
holdings_df["weight"] = holdings_df["weight"].astype(float)
print("\nTop Holdings:")
print(holdings_df.head(10))  # show top 10 holdings
