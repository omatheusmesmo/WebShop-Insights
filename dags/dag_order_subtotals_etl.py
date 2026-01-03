from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# 1. IMPORTAÇÃO DA LÓGICA
# O Airflow adiciona a pasta 'plugins' ao PYTHONPATH, permitindo a importação.
from pipelines.python.order_subtotals import run_pipeline

with DAG(
        dag_id="order_subtotals_etl",
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        schedule="@daily",
        catchup=False,
        tags=["python", "etl", "orders"],
) as dag:
    # 2. CHAMADA DA FUNÇÃO PRINCIPAL
    process_subtotals = PythonOperator(
        task_id="calculate_subtotals_task",
        # Chama a função run_pipeline que contém toda a sua lógica E/T/L
        python_callable=run_pipeline,
    )