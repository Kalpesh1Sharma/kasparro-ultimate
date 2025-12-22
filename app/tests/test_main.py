import pytest
import os
import hashlib
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main import app, get_db
from app.core.database import Base
from app.models.etl import IngestionCheckpoint, CryptoPrice

# --- SETUP TEST DATABASE ---
# We use a separate in-memory SQLite database for tests so we don't wipe your real data
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

# Apply the override to the app
app.dependency_overrides[get_db] = override_get_db
client = TestClient(app)

# Reset DB before tests
Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)

# --- TEST SUITE ---

def test_health_check():
    """P1.4: Verify API Health"""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_stats_endpoint():
    """P1.4: Verify Stats Structure"""
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert "total_jobs_run" in data
    assert "system_status" in data

def test_idempotent_ingestion():
    """
    P1.4: Verify Incremental Ingestion (P1.2 Logic).
    1. Upload File -> Success
    2. Upload Same File -> Skipped
    """
    # Create dummy file
    filename = "app/market_data.csv"
    with open(filename, "w", encoding="utf-8") as f:
        f.write("Ticker,LastPrice\n")
        f.write("TestCoin,100.0\n")

    # 1st Upload
    response1 = client.post("/ingest-csv")
    assert response1.status_code == 200
    assert response1.json()["status"] == "processed"

    # 2nd Upload (Should be skipped)
    response2 = client.post("/ingest-csv")
    assert response2.status_code == 200
    assert response2.json()["status"] == "idempotent_skip"

def test_schema_tagging():
    """
    P1.4: Verify Data Source Tagging (P1.1 Logic).
    """
    # Check the database for the coin we just uploaded
    db = TestingSessionLocal()
    record = db.query(CryptoPrice).filter(CryptoPrice.symbol == "TestCoin").first()
    
    assert record is not None
    assert record.price_usd == 100.0
    assert record.source == "csv_report" # Ensure tag is correct
    db.close()

# Cleanup
def teardown_module(module):
    if os.path.exists("test.db"):
        os.remove("test.db")
    if os.path.exists("app/market_data.csv"):
        os.remove("app/market_data.csv")