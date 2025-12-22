import requests
import csv
from datetime import datetime

# URL for getting all active coins (Free Tier compatible)
URL = "https://api.coinpaprika.com/v1/tickers"

def generate_csv():
    print("--- Fetching Real Data from CoinPaprika ---")
    try:
        response = requests.get(URL)
        response.raise_for_status() # Check for HTTP errors
        data = response.json()
        
        # Take only the top 20 coins to keep the CSV clean
        top_20 = data[:20]

        # Define the file path
        filename = "app/market_data.csv"

        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            # We intentionally use different headers to simulate a "messy" client file
            # Schema Mapping: 'AssetName' -> 'symbol', 'LastPrice' -> 'price_usd'
            writer = csv.writer(file)
            writer.writerow(["AssetName", "Ticker", "LastPrice", "CapturedAt"])

            for coin in top_20:
                writer.writerow([
                    coin["name"],           # e.g., "Bitcoin"
                    coin["symbol"],         # e.g., "BTC"
                    coin["quotes"]["USD"]["price"], 
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ])
        
        print(f"✅ Success! Generated '{filename}' with top 20 live coins.")
        print("   Example row:", top_20[0]["name"], top_20[0]["quotes"]["USD"]["price"])

    except Exception as e:
        print(f"❌ Failed to generate CSV: {e}")

if __name__ == "__main__":
    generate_csv()