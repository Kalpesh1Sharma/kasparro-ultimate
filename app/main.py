import time
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
