from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="hello_kubernetes_world",
    start_date=pendulum.datetime(2025, 9, 14, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["kubernetes", "example"],
) as dag:
    start = EmptyOperator(task_id="start")

    hello_kubernetes_task = KubernetesPodOperator(
        task_id="hello_kubernetes_pod_task",
        name="pod-hello-world",
        namespace="airflow",
        image="bash:latest",
        cmds=["bash", "-c"],
        arguments=["""
        echo '=========================================='
        echo 'Olá, Mundo, a partir de um Pod Kubernetes! (Airflow 3)'
        echo 'Esta tarefa foi acionada pelo Airflow.'
        echo 'Aguardando 5 segundos...'
        sleep 5
        echo 'Tarefa concluída com sucesso!'
        echo '=========================================='
        """],
        do_xcom_push=False,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    end = EmptyOperator(task_id="end")

    start >> hello_kubernetes_task >> end