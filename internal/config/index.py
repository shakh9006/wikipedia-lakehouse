from datetime import timedelta
from dotenv import load_dotenv
import os

load_dotenv('/opt/airflow/.env')

# MINIO S3
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# AIRFLOW
DEFAULT_ARGS = {
    'owner': 'swift',
    'retries': 3,
    'retry_delay': timedelta(hours=1),
}