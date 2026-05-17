from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'silver_gold_etl',
    default_args=default_args,
    description='Daily Batch ETL from Iceberg Bronze -> Silver -> Gold',
    schedule_interval='@daily',
    catchup=False,
    tags=['lakehouse', 'batch', 'fintech'],
) as dag:

    start = EmptyOperator(task_id='start')

    # Run the PySpark script using Python
    # Since PySpark and Java are installed in the Airflow image,
    # and the lakehouse directory is mounted at /opt/airflow/lakehouse,
    # we can run it as a standard python script.
    run_etl_job = BashOperator(
        task_id='run_silver_gold_pyspark_job',
        bash_command='python /opt/airflow/lakehouse/silver_gold_etl.py ',
    )

    end = EmptyOperator(task_id='end')

    # Define DAG flow
    start >> run_etl_job >> end
