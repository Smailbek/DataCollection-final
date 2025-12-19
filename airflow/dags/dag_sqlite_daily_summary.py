import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.db.daily_agg import compute_daily_summary

default_args = {"retries": 1, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id="opensky_sqlite_daily_summary",
    start_date=datetime(2025, 12, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["opensky", "sqlite", "summary"]
) as dag:
    PythonOperator(
        task_id="compute_daily_summary",
        python_callable=compute_daily_summary
    )
