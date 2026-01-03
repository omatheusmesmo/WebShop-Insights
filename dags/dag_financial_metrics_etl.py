from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from pipelines.python.financial_metrics import run_pipeline

with DAG(
        dag_id="financial_metrics_etl",
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        schedule="@daily",
        catchup=False,
        tags=["python", "etl", "financial"],
) as dag:
    process_subtotals = PythonOperator(
        task_id="financial_metrics_task",
         python_callable=run_pipeline,
    )