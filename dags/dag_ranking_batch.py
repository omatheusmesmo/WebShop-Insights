from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sensors.external_task import ExternalTaskSensor
from kubernetes.client import models as k8s

with DAG(
    dag_id="product_ranking_batch_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["java", "batch", "analytics", "kubernetes"],
) as dag:

    wait_for_init =  ExternalTaskSensor(
        task_id = "wait_for_data_population",
        external_dag_id="data_population_etl",
        external_task_id="populate_webshop_db",
        execution_date_fn=lambda dt: pendulum.datetime(2026, 1, 1, tz="UTC"),
    )

    run_java_pipeline = KubernetesPodOperator(
        task_id="run_java_product_ranking_batch",
        name="product-ranking-batch-pod",
        namespace="default",
        image="product-ranking-batch:latest",
        image_pull_policy="IfNotPresent",
        cmds=["java", "-jar", "/app/product-ranking-batch-0.0.1-SNAPSHOT.jar"],
        env_from=[
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="ranking-batch-config")),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="ranking-batch-secret")),
        ],
        is_delete_operator_pod=True,
        get_logs=True,
        log_events_on_failure=True,
    )

    wait_for_init >> run_java_pipeline