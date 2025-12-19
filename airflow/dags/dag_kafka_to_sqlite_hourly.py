import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.consumer.kafka_to_sqlite import consume_and_load

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="opensky_kafka_to_sqlite_hourly",
    start_date=datetime(2025, 12, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["opensky", "kafka", "sqlite"]
) as dag:
    PythonOperator(
        task_id="consume_kafka_clean_and_load_sqlite",
        python_callable=consume_and_load,
        op_kwargs={"group_id": "opensky_hourly_loader", "max_messages": 2000, "max_seconds": 60},
    )
