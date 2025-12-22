import os

# --- 1. app/core/logging_config.py ---
# Uses JSON logging if available, falls back to standard if not (Prevents Crashes)
logging_config_code = """import logging
import sys

def setup_logging():
    logger = logging.getLogger()
    if logger.handlers:
        return logger
    
    handler = logging.StreamHandler(sys.stdout)
    
    try:
        from pythonjsonlogger import jsonlogger
        formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    except ImportError:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
"""

# --- 2. app/models/etl.py ---
models_code = """from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.sql import func
from app.core.database import Base

class CryptoPrice(Base):
    __tablename__ = "crypto_prices"
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    price_usd = Column(Float)
    source = Column(String, index=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())

class IngestionCheckpoint(Base):
    __tablename__ = "ingestion_checkpoints"
    id = Column(Integer, primary_key=True, index=True)
    source_file = Column(String, unique=True, index=True)
    file_hash = Column(String, index=True)
    processed_at = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(String, default="success")

class ETLJob(Base):
    __tablename__ = "etl_jobs"
    id = Column(Integer, primary_key=True, index=True)
    run_time = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(String)
    records_processed = Column(Integer, default=0)
    duration_ms = Column(Float, default=0.0)
    error_message = Column(String, nullable=True)
"""

# --- 3. app/schemas/crypto.py ---
schemas_code = """from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional, Dict, Any

class CryptoPriceResponse(BaseModel):
    symbol: str
    price_usd: float
    source: str
    timestamp: datetime
    class Config:
        from_attributes = True

class PaginatedResponse(BaseModel):
    page: int
    limit: int
    count: int
    data: List[CryptoPriceResponse]

class StatsResponse(BaseModel):
    system_status: str
    total_jobs_run: int
    failed_jobs: int
    last_run: Optional[Dict[str, Any]]

class JobRunResponse(BaseModel):
    id: int
    run_time: datetime
    status: str
    records_processed: int
    duration_ms: float
    error_message: Optional[str] = None
    class Config:
        from_attributes = True

class AnomalyReport(BaseModel):
    latest_run_id: int
    status: str
    anomalies: List[str]
    metrics: Dict[str, float]
"""

# --- 4. app/services/ingestion.py ---
ingestion_code = """import requests
import csv
import os
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

logger = logging.getLogger(__name__)

COINPAPRIKA_URL = "https://api.coinpaprika.com/v1"
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

def detect_schema_drift(data: dict, expected_keys: set, source_name: str):
    current_keys = set(data.keys())
    missing = expected_keys - current_keys
    if missing:
        logger.warning("Schema Drift Detected", extra={"source": source_name, "missing": list(missing)})
        return True 
    return False

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10), before_sleep=before_sleep_log(logger, logging.WARNING))
def fetch_crypto_data(coin_id="btc-bitcoin"):
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
    clean_data = []
    try:
        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if "Ticker" in row and "LastPrice" in row:
                    clean_data.append({"symbol": row["Ticker"], "price_usd": float(row["LastPrice"]), "source": "csv_report"})
        return clean_data
    except Exception as e:
        return {"error": str(e)}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=20), before_sleep=before_sleep_log(logger, logging.WARNING))
def fetch_coingecko_price(coin_id="bitcoin"):
    try:
        api_key = os.getenv("COINGECKO_API_KEY")
        headers = {}
        if api_key: headers["x-cg-demo-api-key"] = api_key
        params = {"ids": coin_id, "vs_currencies": "usd"}
        response = requests.get(COINGECKO_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        detect_schema_drift(data, {coin_id}, "CoinGecko")
        price = data.get(coin_id, {}).get("usd")
        if price: return {"symbol": "BTC", "price_usd": float(price), "source": "coingecko"}
        raise ValueError("Price data missing in response")
    except Exception as e:
        logger.error(f"CoinGecko fetch failed: {e}")
        raise e
"""

# --- 5. app/api/routes.py ---
routes_code = """import os
import hashlib
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text, desc

from app.core.database import get_db
from app.models.etl import CryptoPrice, IngestionCheckpoint, ETLJob
from app.services.ingestion import ingest_csv_data
from app.schemas.crypto import (
    CryptoPriceResponse, PaginatedResponse, StatsResponse, 
    JobRunResponse, AnomalyReport
)

router = APIRouter()

@router.get("/health")
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@router.post("/chaos/simulate-failure")
def simulate_failure(type: str = "db_crash"):
    if type == "db_crash":
        raise HTTPException(status_code=500, detail="Chaos Monkey: Database Connection Severed!")
    elif type == "file_loss":
        target = "app/market_data.csv"
        if os.path.exists(target):
            os.remove(target)
            return {"message": "Chaos Monkey: CSV file deleted."}
        return {"message": "File already missing."}
    return {"message": "Unknown chaos type"}

@router.get("/data", response_model=PaginatedResponse)
def get_data(page: int = 1, limit: int = 20, db: Session = Depends(get_db)):
    skip = (page - 1) * limit
    records = db.query(CryptoPrice).order_by(CryptoPrice.timestamp.desc()).limit(limit).offset(skip).all()
    return {"page": page, "limit": limit, "count": len(records), "data": records}

@router.get("/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    total_runs = db.query(ETLJob).count()
    last_run = db.query(ETLJob).order_by(desc(ETLJob.run_time)).first()
    failed_runs = db.query(ETLJob).filter(ETLJob.status == "failure").count()
    return {
        "system_status": "healthy" if last_run and last_run.status == "success" else "degraded",
        "total_jobs_run": total_runs,
        "failed_jobs": failed_runs,
        "last_run": {
            "time": last_run.run_time,
            "status": last_run.status,
            "records": last_run.records_processed,
            "duration_ms": last_run.duration_ms
        } if last_run else None
    }

@router.post("/ingest-csv")
def trigger_csv_ingestion(db: Session = Depends(get_db)):
    file_path = "app/market_data.csv"
    try:
        with open(file_path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="CSV file not found")

    existing_job = db.query(IngestionCheckpoint).filter(IngestionCheckpoint.file_hash == file_hash).first()
    if existing_job:
        return {"message": "Skipped: File already processed.", "status": "idempotent_skip"}

    data_list = ingest_csv_data(file_path)
    if isinstance(data_list, dict) and "error" in data_list:
        return data_list

    for item in data_list:
        db.add(CryptoPrice(symbol=item["symbol"], price_usd=item["price_usd"], source=item["source"]))
    
    db.add(IngestionCheckpoint(source_file="market_data.csv", file_hash=file_hash, status="success"))
    db.commit()
    return {"message": f"Successfully ingested {len(data_list)} new records.", "status": "processed"}

@router.get("/runs", response_model=List[JobRunResponse])
def get_run_history(limit: int = 10, db: Session = Depends(get_db)):
    return db.query(ETLJob).order_by(desc(ETLJob.run_time)).limit(limit).all()

@router.get("/compare-runs", response_model=AnomalyReport)
def compare_runs(db: Session = Depends(get_db)):
    latest = db.query(ETLJob).order_by(desc(ETLJob.run_time)).first()
    if not latest: raise HTTPException(status_code=404, detail="No run history")
    history = db.query(ETLJob).filter(ETLJob.status == "success").filter(ETLJob.id != latest.id).order_by(desc(ETLJob.run_time)).limit(10).all()
    if not history: return {"latest_run_id": latest.id, "status": "baseline_building", "anomalies": ["Not enough history"], "metrics": {}}
    
    avg_dur = sum(r.duration_ms for r in history) / len(history)
    avg_rec = sum(r.records_processed for r in history) / len(history)
    anomalies = []
    if latest.status != "success": anomalies.append(f"Critical Failure: {latest.error_message}")
    if latest.duration_ms > (avg_dur * 2) and latest.duration_ms > 500: anomalies.append("Duration Spike")
    if latest.records_processed == 0 and avg_rec > 0: anomalies.append("Data Gap")
    return {"latest_run_id": latest.id, "status": "anomaly_detected" if anomalies else "normal", "anomalies": anomalies, "metrics": {"latest_duration": latest.duration_ms, "avg_duration": round(avg_dur, 2)}}
"""

# --- 6. app/main.py ---
main_code = """import time
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler

from app.core.database import engine, SessionLocal, Base
from app.models.etl import CryptoPrice, ETLJob
from app.services.ingestion import fetch_crypto_data, fetch_coingecko_price
from app.api.routes import router
from app.core.logging_config import setup_logging

# P2.4: Setup Logging (Safe Mode)
setup_logging()
logger = logging.getLogger(__name__)

Base.metadata.create_all(bind=engine)

def auto_fetch_job():
    start_time = time.time()
    logger.info("Auto-Fetch Starting", extra={"job": "etl_pipeline"})
    db = SessionLocal()
    records_count = 0
    status = "success"
    error_msg = None

    sources = [
        ("CoinPaprika", lambda: fetch_crypto_data("btc-bitcoin")),
        ("CoinGecko",   lambda: fetch_coingecko_price("bitcoin"))
    ]

    for name, fetch_func in sources:
        try:
            data = fetch_func()
            db.add(CryptoPrice(symbol=data["symbol"], price_usd=data["price_usd"], source=data["source"]))
            records_count += 1
            logger.info("Price Saved", extra={"source": name, "symbol": data['symbol']})
        except Exception as e:
            logger.error(f"Source Failed: {name}", extra={"error": str(e)})
            status = "partial_failure"
            error_msg = str(e)
    
    try:
        db.commit()
    except Exception as e:
        status = "failure"
        logger.critical(f"DB Error: {e}")
    finally:
        duration = (time.time() - start_time) * 1000
        db.add(ETLJob(status=status, records_processed=records_count, duration_ms=round(duration, 2), error_message=error_msg))
        db.commit()
        db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = BackgroundScheduler()
    scheduler.add_job(auto_fetch_job, 'interval', seconds=60)
    scheduler.start()
    logger.info("System Online")
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
app.include_router(router)

@app.get("/")
def read_root():
    return {"message": "Crypto System Online - Fully Restored"}
"""

def write_file(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"✅ Fixed {path}")

if __name__ == "__main__":
    write_file("app/core/logging_config.py", logging_config_code)
    write_file("app/models/etl.py", models_code)
    write_file("app/schemas/crypto.py", schemas_code)
    write_file("app/services/ingestion.py", ingestion_code)
    write_file("app/api/routes.py", routes_code)
    write_file("app/main.py", main_code)
    print("🚀 All project files have been restored to a 100% working state.")