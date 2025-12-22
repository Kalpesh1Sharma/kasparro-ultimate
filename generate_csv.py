import pandas as pd
import requests
import os
from datetime import datetime

def run_etl():
    # CoinGecko API URL for Bitcoin
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        'ids': 'bitcoin',
        'vs_currencies': 'usd',
        'include_24hr_vol': 'true'
    }
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        
        # Extract data with safe defaults
        # Ensure these exact keys are used when creating the CSV
        new_row = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'price_usd': data['bitcoin']['usd'],
            'volume_24h': data['bitcoin']['usd_24h_vol']
        }
        
        df_new = pd.DataFrame([new_row])
        
        # Append to existing CSV or create new one
        file_path = 'crypto_data.csv'
        if os.path.exists(file_path):
            df_existing = pd.read_csv(file_path)
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_final = df_new
            
        df_final.to_csv(file_path, index=False)
        print(f"Successfully updated {file_path}")
        
    except Exception as e:
        print(f"ETL Error: {e}")

if __name__ == "__main__":
    run_etl()
