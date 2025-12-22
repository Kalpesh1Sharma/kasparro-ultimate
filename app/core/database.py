import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# FIX: Read the connection URL from the environment variable (defined in docker-compose.yml)
# If it's missing, fall back to the string we know works for your setup.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:postgres@db:5432/kasparro_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()