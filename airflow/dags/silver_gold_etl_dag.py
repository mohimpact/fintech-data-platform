from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1, tz="UTC"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": duration(minutes=5),
}

with DAG(
    "silver_gold_etl",
    default_args=default_args,
    description="Daily batch ETL from Iceberg Bronze to Silver and Gold",
    schedule="@daily",
    catchup=False,
    tags=["lakehouse", "batch", "fintech"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_etl_job = BashOperator(
        task_id="run_silver_gold_pyspark_job",
        bash_command="python -m fintech_platform.lakehouse.silver_gold",
    )

    end = EmptyOperator(task_id="end")

    start >> run_etl_job >> end
