import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Settings:
    # API Settings
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))

    # BigQuery Settings
    BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "ex-whitestork")
    BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "staging")
    BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "every_action_data_staging_email_test")

    # Email Settings
    EMAIL_TO = os.getenv("EMAIL_TO", "data.analysis@exacti.us")

# Create settings instance
settings = Settings() 