from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pipelines.python.order_subtotals import run_pipeline

with DAG(
        dag_id="order_subtotals_etl",
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        schedule="@daily",
        catchup=False,
        tags=["python", "etl", "orders"],
) as dag:

    wait_for_init = ExternalTaskSensor(
        task_id="wait_for_data_population",
        external_dag_id="data_population_etl",
        external_task_id="populate_webshop_db",
        execution_date_fn=lambda dt: pendulum.datetime(2026, 1, 1, tz="UTC"),
    )

    process_subtotals = PythonOperator(
        task_id="calculate_subtotals_task",
        python_callable=run_pipeline,
    )

    trigger_financial = TriggerDagRunOperator(
        task_id="trigger_financial_metrics",
        trigger_dag_id="financial_metrics_etl"
    )

    wait_for_init >> process_subtotals >> trigger_financial