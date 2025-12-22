import requests
import csv
import os
import logging
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
from sqlalchemy.orm import Session
from app.core.config import settings # Import configuration settings securely
from app.core.database import get_db
from app.models.etl import ETLJob, CryptoPrice # Import your database models

logger = logging.getLogger(__name__)
# Set level for better visibility in Cloud Logging
logging.basicConfig(level=logging.INFO)

COINPAPRIKA_URL = "https://api.coinpaprika.com/v1"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

def detect_schema_drift(data: dict, expected_keys: set, source_name: str):
    """Checks for missing keys in the API response (Schema Drift, Requirement 4)."""
    current_keys = set(data.keys())
    missing = expected_keys - current_keys
    if missing:
        logger.warning("Schema Drift Detected", extra={"source": source_name, "missing": list(missing)})
        return True 
    return False

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10), before_sleep=before_sleep_log(logger, logging.WARNING))
def fetch_crypto_data(coin_id="btc-bitcoin"):
    """Fetches data from CoinPaprika with Retry (Failure Recovery, Requirement 4)."""
    try:
        url = f"{COINPAPRIKA_URL}/tickers/{coin_id}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        detect_schema_drift(data, {"symbol", "quotes", "name"}, "CoinPaprika")
        return {"symbol": data.get("symbol"), "price_usd": float(data["quotes"]["USD"]["price"]), "source": "coinpaprika"}
    except Exception as e:
        logger.error(f"CoinPaprika fetch failed: {e}")
        raise e 

def ingest_csv_data(file_path: str):
    """Ingests local CSV data with ETL transformations (Requirement 4)."""
    clean_data = []
    try:
        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Basic ETL Transformation: check for required columns
                if "Ticker" in row and "LastPrice" in row:
                    clean_data.append({"symbol": row["Ticker"], "price_usd": float(row["LastPrice"]), "source": "csv_report"})
        return clean_data
    except Exception as e:
        return {"error": str(e)}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=20), before_sleep=before_sleep_log(logger, logging.WARNING))
def fetch_coingecko_price(coin_id="bitcoin"):
    """
    Fetches data from CoinGecko with Authentication and Retry (Requirement 1 & 4).
    """
    try:
        # Securely retrieve API Key from settings (Phase 1)
        api_key = settings.API_KEY 
        
        headers = {}
        if api_key: 
            # Use the provided key for rate-limited/authenticated access if available
            headers["x-cg-demo-api-key"] = api_key
            logger.info("Using authenticated CoinGecko API key.")
        
        params = {"ids": coin_id, "vs_currencies": "usd"}
        response = requests.get(COINGECKO_URL, params=params, headers=headers, timeout=10)
        
        # Check for rate limiting status (Requirement 4)
        if response.status_code == 429:
            logger.error("Rate Limit Exceeded (429) on CoinGecko API.")
            raise requests.HTTPError("Rate Limit Exceeded")
            
        response.raise_for_status()
        data = response.json()
        
        # Check for schema drift on the response
        detect_schema_drift(data, {coin_id}, "CoinGecko")
        
        price = data.get(coin_id, {}).get("usd")
        if price: 
            return {"symbol": "BTC", "price_usd": float(price), "source": "coingecko"}
            
        raise ValueError("Price data missing in response")
    except Exception as e:
        logger.error(f"CoinGecko fetch failed: {e}")
        raise e

# --- NEW REQUIRED FUNCTIONS FOR SCHEDULER/API TRIGGER ---

def run_etl_job(db: Session):
    """
    Core function that executes the CoinGecko ETL pipeline and logs its status.
    This fulfills ETL transformations, Failure Recovery, and Logs/Metrics (Req 4 & 6).
    """
    start_time = datetime.now()
    job_log = ETLJob(status="running", records_processed=0)
    db.add(job_log)
    db.commit()
    db.refresh(job_log)
    
    logger.info(f"ETL Job ID {job_log.id}: Starting CoinGecko live fetch.")

    new_records = 0
    
    try:
        # 1. Extraction: Fetch data from the live API
        live_data = fetch_coingecko_price(coin_id="bitcoin")
        
        # 2. Transformation/Load:
        # Only process if data is received (Incremental Ingestion logic)
        if live_data and live_data.get("price_usd"):
            # Insert the new record
            db.add(CryptoPrice(**live_data))
            new_records = 1
        
        db.commit() 
        
        # 3. Log Success
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() * 1000 
        
        job_log.status = "success"
        job_log.records_processed = new_records
        job_log.duration_ms = duration
        db.commit()
        
        logger.info(f"ETL Job ID {job_log.id}: Success. Processed {new_records} record(s) in {duration:.2f}ms.")
        return new_records

    except Exception as e:
        # Failure Recovery Logic: Rollback transaction and log the failure
        db.rollback() 
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() * 1000

        job_log.status = "failed"
        job_log.error_message = str(e)
        job_log.duration_ms = duration
        db.commit()
        
        logger.error(f"ETL Job ID {job_log.id}: FAILED. Error: {e}", exc_info=True)
        # Re-raise the exception so the Cloud Scheduler route catches it (Optional: but useful for logs)
        raise

def run_etl_with_db_session():
    """
    Wrapper function to manage the SQLAlchemy session lifecycle for the API route/Scheduler.
    """
    # Use the generator function from app/core/database.py to ensure the session is properly closed
    db_generator = get_db()
    db = next(db_generator)
    
    try:
        # Execute the core job logic
        run_etl_job(db)
    finally:
        # Ensure the session is closed even if an exception occurs
        db_generator.close()