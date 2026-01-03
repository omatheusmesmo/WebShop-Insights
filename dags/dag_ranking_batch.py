from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id="product_ranking_batch_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["java", "batch", "analytics", "kubernetes"],
) as dag:
    # Este operador irá criar um Pod no Kubernetes para executar a aplicação Java.
    # O Pod é temporário e será destruído após a conclusão da tarefa.
    run_java_pipeline = KubernetesPodOperator(
        task_id="run_java_product_ranking_batch",
        name="product-ranking-batch-pod",  # Nome do Pod que será criado no Kubernetes
        namespace="default",

        # IMPORTANTE: Esta deve ser a imagem Docker criada a partir do seu projeto Java.
        # Certifique-se de que a imagem foi carregada no Minikube, por exemplo com:
        # minikube image build -t product-ranking-batch:latest -f pipelines/java/product-ranking-batch/Dockerfile .
        image="product-ranking-batch:latest",
        image_pull_policy="IfNotPresent",

        # Comando para executar a aplicação dentro do container.
        # O Dockerfile da sua aplicação Java deve copiar o JAR para este caminho.
        cmds=["java", "-jar", "/app/product-ranking-batch-0.0.1-SNAPSHOT.jar"],

        # Anexa os ConfigMaps e Secrets necessários como variáveis de ambiente ao Pod.
        # Os nomes 'ranking-batch-config' e 'ranking-batch-secret' são definidos
        # nos seus arquivos YAML na pasta 'infra/'.
        env_from=[
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="ranking-batch-config")),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="ranking-batch-secret")),
        ],

        # Garante que o Pod seja excluído após a conclusão (sucesso ou falha).
        is_delete_operator_pod=True,

        # Permite que o Airflow capture e exiba os logs do pod Java.
        get_logs=True,
        log_events_on_failure=True,
    )