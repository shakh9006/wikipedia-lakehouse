import sys

sys.path.append('/opt/airflow/internal')

from config.index import S3_BRONZE_PREFIX

def get_s3_key(dt):
    return (f"{S3_BRONZE_PREFIX}/year={dt.year}/month={dt.month:02d}/"
            f"day={dt.day:02d}/hour={dt.hour:02d}/pageviews-{dt.strftime('%Y%m%d-%H0000')}.gz")