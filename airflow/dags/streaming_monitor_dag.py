import logging
import urllib.request

from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1, tz="UTC"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def check_minio_health():
    """Verify MinIO is reachable from Airflow."""
    try:
        url = "http://minio:9000/minio/health/live"
        response = urllib.request.urlopen(url, timeout=5)
        if response.status == 200:
            logging.info("MinIO is alive and healthy.")
        else:
            raise ValueError(f"MinIO health check returned status code: {response.status}")
    except Exception as e:
        logging.error(f"Failed to connect to MinIO: {e}")
        raise


with DAG(
    "streaming_monitor_heartbeat",
    default_args=default_args,
    description="Monitors the health of the streaming infrastructure",
    schedule="*/5 * * * *",
    catchup=False,
    tags=["monitoring", "streaming"],
) as dag:

    health_check = PythonOperator(
        task_id="minio_heartbeat_check",
        python_callable=check_minio_health,
    )
