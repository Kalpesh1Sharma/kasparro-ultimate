# Kasparro Crypto Sentinel: Cloud-Ready ETL & API

An automated ETL (Extract, Transform, Load) pipeline and RESTful API designed for high-frequency cryptocurrency data ingestion and analytics.



## 🎯 Project Overview
This project demonstrates a production-grade backend architecture. It fetches live market data from the **CoinGecko API**, processes it via a **FastAPI** backend, and persists it in a **PostgreSQL** database. The system is fully containerized and designed for serverless cloud deployment.

## 🛠️ Tech Stack
* **Language:** Python 3.11
* **Framework:** FastAPI (Asynchronous API)
* **Database:** PostgreSQL
* **Containerization:** Docker & Docker Compose
* **Cloud Architecture (Designed):** AWS ECS Fargate, ECR, Amazon RDS
* **Visualization:** Streamlit

## 🚀 Key Features
* **Automated ETL:** Scheduled ingestion of crypto price and volume data.
* **Containerized Environment:** Environment-agnostic deployment using Docker.
* **Cloud-Ready:** Includes task definitions for AWS Fargate and RDS connectivity.
* **Interactive Dashboard:** Streamlit-based UI for data visualization.

## 📦 Local Setup
1. Clone the repository:
   ```bash
   git clone [https://github.com/your-username/kasparro-ultimate.git](https://github.com/your-username/kasparro-ultimate.git)
   cd kasparro-ultimate 
   ```
Set up your .env file with your COINGECKO_API_KEY.

Launch the entire system using Docker Compose:

Bash

docker-compose up --build
Access the API at http://localhost:8000 and the Dashboard at http://localhost:8501.

☁️ AWS Architecture Deployment (Design)
The system is architected for AWS Fargate to ensure serverless scalability.

CI/CD: Docker images are pushed to Amazon ECR.

Compute: ECS Fargate runs the API and ETL tasks.

Database: Amazon RDS (PostgreSQL) handles data persistence.

Scheduling: Amazon EventBridge triggers the ETL endpoint on a cron schedule.