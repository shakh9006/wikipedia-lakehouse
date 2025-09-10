import boto3
from botocore.client import Config
import logging
import requests
import sys

from datetime import datetime, timedelta

sys.path.append('/opt/airflow/internal')

from config.index import S3_BRONZE_BUCKET, MINIO_CLIENT, MINIO_REGION, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from scripts.get_s3_key import get_s3_key

def fetch_latest_available(start_date):
    try:
        logging.info(f"Fetching latest available for date: {start_date}")

        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_CLIENT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=MINIO_REGION,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

        timeout = 10
        dt = start_date - timedelta(hours=2)
        url = dt.strftime("https://dumps.wikimedia.org/other/pageviews/%Y/%Y-%m/pageviews-%Y%m%d-%H0000.gz")

        logging.info(f"Checking for url: {url}")

        head = requests.head(url, timeout=timeout)
        if head.status_code != 200:
            logging.error(f"Not found: {head.status_code} for date: {dt}")
            return None

        resp = requests.get(url, stream=True, timeout=timeout)
        resp.raise_for_status()
        key = get_s3_key(dt)
        s3.upload_fileobj(Fileobj=resp.raw, Bucket=S3_BRONZE_BUCKET, Key=key, ExtraArgs={"ContentType": "application/gzip"})
        logging.info(f"Uploaded to s3://{S3_BRONZE_BUCKET}/{key}")
    except Exception as e:
        logging.error(f"Error occurred while fetching latest available: {e}")
        return None

    