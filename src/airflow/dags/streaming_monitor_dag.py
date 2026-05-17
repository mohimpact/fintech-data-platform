from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import urllib.request
import logging

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def check_minio_health():
    """
    A simple heartbeat check to verify MinIO is accessible.
    In a real-world scenario, this would check if the streaming
    checkpoint's last modified timestamp is recent, ensuring the
    stream is actively processing data.
    """
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
    'streaming_monitor_heartbeat',
    default_args=default_args,
    description='Monitors the health of the streaming infrastructure',
    schedule_interval='*/5 * * * *', # Run every 5 minutes
    catchup=False,
    tags=['monitoring', 'streaming'],
) as dag:

    health_check = PythonOperator(
        task_id='minio_heartbeat_check',
        python_callable=check_minio_health,
    )
