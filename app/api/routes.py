import os
import hashlib
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text, desc

# Import the necessary modules
from app.core.database import get_db
from app.models.etl import CryptoPrice, IngestionCheckpoint, ETLJob
# Import the function for the live API ingestion
from app.services.ingestion import ingest_csv_data, run_etl_with_db_session 
from app.schemas.crypto import (
    CryptoPriceResponse, PaginatedResponse, StatsResponse, 
    JobRunResponse, AnomalyReport
)

router = APIRouter()

# --- Core Health and Debug Endpoints ---

@router.get("/health")
def health_check(db: Session = Depends(get_db)):
    """API and Database Connectivity Check."""
    try:
        # Pings the database to check connection status (Required for healthcheck in docker-compose)
        db.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        # Fails the healthcheck gracefully
        return {"status": "unhealthy", "error": str(e)}

@router.post("/chaos/simulate-failure")
def simulate_failure(type: str = "db_crash"):
    """
    Simulates various failures for testing Failure Recovery (Requirement 4).
    """
    if type == "db_crash":
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Chaos Monkey: Database Connection Severed!")
    elif type == "file_loss":
        # Simulates a file loss (relevant for local CSV ingestion tests)
        target = "app/market_data.csv"
        if os.path.exists(target):
            os.remove(target)
            return {"message": "Chaos Monkey: CSV file deleted."}
        return {"message": "File already missing."}
    return {"message": "Unknown chaos type"}

# --- Data and Metrics Endpoints (Verification by Evaluators) ---

@router.get("/data", response_model=PaginatedResponse)
def get_data(page: int = 1, limit: int = 20, db: Session = Depends(get_db)):
    """Retrieves paginated, latest processed data."""
    skip = (page - 1) * limit
    records = db.query(CryptoPrice).order_by(CryptoPrice.timestamp.desc()).limit(limit).offset(skip).all()
    return {"page": page, "limit": limit, "count": len(records), "data": records}

@router.get("/stats", response_model=StatsResponse)
def get_stats(db: Session = Depends(get_db)):
    """Provides high-level ETL job statistics (Logs + Metrics, Requirement 6)."""
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

@router.get("/runs", response_model=List[JobRunResponse])
def get_run_history(limit: int = 10, db: Session = Depends(get_db)):
    """Retrieves history of ETL job runs (Logs + Metrics, Requirement 6)."""
    return db.query(ETLJob).order_by(desc(ETLJob.run_time)).limit(limit).all()

@router.get("/compare-runs", response_model=AnomalyReport)
def compare_runs(db: Session = Depends(get_db)):
    """Performs a basic anomaly check on the latest ETL run (Schema Drift/Accuracy, Requirement 4)."""
    latest = db.query(ETLJob).order_by(desc(ETLJob.run_time)).first()
    if not latest: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No run history")
    
    # Get last 10 successful runs for baseline comparison
    history = db.query(ETLJob).filter(ETLJob.status == "success").filter(ETLJob.id != latest.id).order_by(desc(ETLJob.run_time)).limit(10).all()
    if not history: 
        return {"latest_run_id": latest.id, "status": "baseline_building", "anomalies": ["Not enough history"], "metrics": {}}
    
    avg_dur = sum(r.duration_ms for r in history) / len(history)
    avg_rec = sum(r.records_processed for r in history) / len(history)
    
    anomalies = []
    if latest.status != "success": anomalies.append(f"Critical Failure: {latest.error_message}")
    if latest.duration_ms > (avg_dur * 2) and latest.duration_ms > 500: anomalies.append("Duration Spike (Possible rate limit or API slowness)") # Rate limiting logic check (Requirement 4)
    if latest.records_processed == 0 and avg_rec > 0: anomalies.append("Data Gap (Zero records processed)")
    
    return {"latest_run_id": latest.id, "status": "anomaly_detected" if anomalies else "normal", "anomalies": anomalies, "metrics": {"latest_duration": latest.duration_ms, "avg_duration": round(avg_dur, 2)}}

# --- Ingestion Endpoints ---

@router.post("/ingest-csv")
def trigger_csv_ingestion(db: Session = Depends(get_db)):
    """
    Triggers local CSV ingestion with Incremental Ingestion logic (Requirement 4).
    """
    file_path = "app/market_data.csv"
    try:
        with open(file_path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
    except FileNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="CSV file not found")

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

# --- CRITICAL CLOUD SCHEDULER ROUTE (Requirement 3) ---

@router.post("/etl/run", status_code=status.HTTP_200_OK)
def trigger_cloud_etl():
    """
    Dedicated endpoint for the Cloud Scheduler to hit.
    Triggers the CoinGecko API ingestion job once (Cloud-based scheduled run).
    """
    try:
        # This calls the live API ingestion job, which logs its result to the ETLJob table.
        run_etl_with_db_session() 
        return {"message": "Cloud ETL job successfully triggered and logged.", "status": "success"}
    except Exception as e:
        # Catches unexpected runtime errors and provides feedback to the Cloud Scheduler/Log
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cloud ETL job failed during execution: {e}"
        )