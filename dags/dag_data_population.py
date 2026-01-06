from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum

from pipelines.python.insert_webshop import run_pipeline

with DAG(
        dag_id="data_population_etl",
        schedule="@once",
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        tags=['setup', 'data_ingestion'],
        doc_md=__doc__,
) as dag:
    populate_webshop_task = PythonOperator(
        task_id='populate_webshop_db',
        python_callable=run_pipeline,
        do_xcom_push=False,
    )