from pydantic import BaseModel
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
