import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from src.producer.opensky_to_kafka import run_once

KAFKA_BOOTSTRAP = "kafka:9092"

def task_send_once():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        run_once(producer)
    finally:
        producer.flush()
        producer.close()

default_args = {"retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="opensky_api_to_kafka",
    start_date=datetime(2025, 12, 1),
    schedule="*/2 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["opensky", "kafka", "api"]
) as dag:
    PythonOperator(
        task_id="fetch_opensky_and_publish",
        python_callable=task_send_once
    )