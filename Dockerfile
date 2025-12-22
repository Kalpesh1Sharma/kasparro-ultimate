# Stage 1: Build Stage (Dependency Installation)
# Use a slim image for a smaller final size
FROM python:3.11-slim as builder

# Set the working directory inside the container
WORKDIR /app

# Install PostgreSQL client libraries needed by psycopg2 to compile/install
# and 'netcat-openbsd' (nc) for the database wait script in entrypoint.sh.
# The 'entrypoint.sh' needs 'psql' for the database wait check.
# Since your entrypoint.sh uses psql, we must install the postgres client.
# Note: AWS/GCP services like Fargate/Cloud Run generally recommend using only a single process, 
# but for the evaluation, an ENTRYPOINT is best for a reliable startup.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    postgresql-client \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt and install dependencies first for caching layers
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Final Production Stage (Minimal Image)
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED 1
WORKDIR /app

# Copy the installed packages from the builder stage
# This creates a smaller final image by avoiding the build tools
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy the postgresql-client (psql) binary and libraries needed for entrypoint.sh
COPY --from=builder /usr/bin/psql /usr/bin/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libpq.so.5 /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/bin/nc.openbsd /usr/bin/nc
COPY --from=builder /usr/lib/locale/ /usr/lib/locale/

# Copy all application code, including the main app folder, Dockerfile, and entrypoint.sh
COPY . /app

# Make the entrypoint script executable (Crucial step!)
RUN chmod +x /app/entrypoint.sh

# Expose the API port (8000)
EXPOSE 8000

# ENTRYPOINT executes the script first. CMD provides arguments (default). 
# This is the most reliable way to ensure the startup script runs.
ENTRYPOINT ["/app/entrypoint.sh"] 

# CMD is not strictly needed here as the entrypoint script does the final 'exec' 
# to run the API, but having it explicitly states the intent.
CMD ["/bin/bash"]