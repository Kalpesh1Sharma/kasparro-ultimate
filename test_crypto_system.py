import requests
import json

# 1. Your Local System (The "Proxy")
# Default Uvicorn port is usually 8000. Change if yours is different.
LOCAL_URL = "http://127.0.0.1:8000"

# 2. The Upstream Source (CoinPaprika)
# This is the public API your team likely uses.
COINPAPRIKA_URL = "https://api.coinpaprika.com/v1"

def check_local_system():
    """Checks your local Kasparro/Crypto System"""
    print(f"--- Connecting to Local System ({LOCAL_URL}) ---")
    try:
        response = requests.get(LOCAL_URL)
        if response.status_code == 200:
            print("✅ Local System is ONLINE")
            print(f"   Response: {response.json()}")
        else:
            print(f"❌ Local System returned error: {response.status_code}")
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to Local System. Is Uvicorn running?")

def check_upstream_data():
    """Checks the CoinPaprika API directly"""
    print(f"\n--- Connecting to CoinPaprika API ({COINPAPRIKA_URL}) ---")
    try:
        # 'global' is a standard endpoint for market overview
        response = requests.get(f"{COINPAPRIKA_URL}/global")
        if response.status_code == 200:
            data = response.json()
            market_cap = data.get('market_cap_usd', 0)
            print("✅ CoinPaprika API is ONLINE")
            print(f"   Global Crypto Market Cap: ${market_cap:,.0f}")
        else:
            print(f"⚠️ CoinPaprika Error: {response.status_code}")
    except Exception as e:
        print(f"❌ Failed to connect to CoinPaprika: {e}")

def get_bitcoin_price_direct():
    """Example of fetching raw data from CoinPaprika"""
    print("\n--- Fetching Bitcoin Price (Direct from Source) ---")
    # Endpoint for a specific coin. ID for Bitcoin is usually 'btc-bitcoin'
    url = f"{COINPAPRIKA_URL}/tickers/btc-bitcoin"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        name = data.get('name')
        price = data['quotes']['USD']['price']
        print(f"   {name} Price: ${price:,.2f}")
    else:
        print("   Could not fetch Bitcoin price.")

if __name__ == "__main__":
    check_local_system()
    check_upstream_data()
    get_bitcoin_price_direct()