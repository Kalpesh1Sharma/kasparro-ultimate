#!/bin/bash
# entrypoint.sh - Manages startup for Docker container

# Exit immediately if a command exits with a non-zero status.
set -e

echo "--- 1. Waiting for Database to be Ready ---"
# This loop waits for the PostgreSQL service (named 'db' in docker-compose.yml)
# to be fully up and ready before attempting migrations.
# This prevents race conditions and is crucial for reliable startup.
/usr/bin/env sh -c '
until PGPASSWORD=postgres psql -h db -U user -d kasparro_db -c "\q"; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done
>&2 echo "Postgres is up and running on port 5432!"
'

echo "--- 2. Applying Database Schema (Models) ---"
# This command runs a single Python statement to create all tables 
# defined in your app/models/etl.py using the declarative Base.
# This ensures a 'fresh' or recovered database state on startup.
python -c "from app.core.database import Base, engine; Base.metadata.create_all(bind=engine)"
echo "Database schema creation complete."

echo "--- 3. Starting ETL Scheduler Service ---"
# The '&' runs the scheduler in the background. 
# It uses the logic from the run_etl_with_db_session we defined earlier 
# to run the ETL job periodically (e.g., every 30 minutes).
python app/services/scheduler.py &
ETL_PID=$!
echo "ETL Scheduler started with PID: $ETL_PID"

echo "--- 4. Starting API Web Server (Foreground) ---"
# 'exec' replaces the current shell with the Gunicorn process.
# This keeps the main process (PID 1) running the web server, 
# ensuring the container stays alive and exposes the API.
# We use gunicorn to run uvicorn workers for production-grade stability.
exec gunicorn -w 4 -k uuvicorn.workers.UvicornWorker app.main:app -b 0.0.0.0:8000