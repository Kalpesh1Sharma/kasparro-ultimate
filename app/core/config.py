# app/core/config.py
import os
from dotenv import load_dotenv


load_dotenv() 

class Settings:
    """
    Centralized configuration class for the application.
    All environment variables are accessed here to maintain security 
    and provide type-checking/validation.
    """
    PROJECT_NAME: str = "Kasparro ETL"
    
   
    API_KEY: str = os.getenv("API_KEY")

    DATABASE_URL: str = os.getenv("DATABASE_URL")
  
    def __init__(self):
        """
        Validates critical settings on application startup.
        This ensures the system "fails fast" if a mandatory secret is missing.
        """
        
        if not self.API_KEY:
          
            raise ValueError(
                "FATAL ERROR: API_KEY (CoinGecko Key) not found in environment variables. "
                "Ensure it is set in the root .env file or passed via Docker Compose."
            )
        
        
        if not self.DATABASE_URL:
          
            print("WARNING: DATABASE_URL is not explicitly set in the environment.")


settings = Settings()

