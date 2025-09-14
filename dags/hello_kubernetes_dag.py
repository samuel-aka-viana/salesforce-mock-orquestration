from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id="kpo_minimo",
    start_date=pendulum.datetime(2025, 9, 14, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:
    KubernetesPodOperator(
        task_id="echo_kpo",
        namespace="airflow",
        service_account_name="airflow",
        image="bash:5.2",
        cmds=["/bin/bash", "-c"],
        arguments=["echo Hello Kubernetes from KPO!"],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )
