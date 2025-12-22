import pandas as pd
import requests
import os
from datetime import datetime

def run_etl():
    # Use CoinGecko for reliable market data
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {'ids': 'bitcoin', 'vs_currencies': 'usd', 'include_24hr_vol': 'true'}
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        
        # Enforce column naming convention to match Dashboard
        new_row = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'price_usd': data['bitcoin']['usd'],
            'volume_24h': data['bitcoin']['usd_24h_vol']
        }
        
        df_new = pd.DataFrame([new_row])
        file_path = 'crypto_data.csv'
        
        if os.path.exists(file_path):
            df_existing = pd.read_csv(file_path)
            # Maintain data history by appending
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_final = df_new
            
        df_final.to_csv(file_path, index=False)
        print(f"Data ingested successfully into {file_path}")
        
    except Exception as e:
        print(f"Ingestion Failure: {e}")

if __name__ == "__main__":
<<<<<<< HEAD
    run_etl()
=======
    run_etl()
>>>>>>> 895b703b7027cb69fd7907bcf327b3bd0212a2be
