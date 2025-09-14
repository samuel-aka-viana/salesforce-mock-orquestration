from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
        dag_id="kpo_minimo",
        start_date=pendulum.datetime(2025, 9, 14, tz="UTC"),
        schedule=None,
        catchup=False,
        tags=["kubernetes", "test"],
        max_active_runs=1,
) as dag:
    echo_task = KubernetesPodOperator(
        task_id="echo_kpo",
        namespace="airflow",
        service_account_name="airflow",
        image="bash:5.2",
        cmds=["/bin/bash", "-c"],
        arguments=["echo 'Hello Kubernetes from KPO!' && sleep 10"],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=600,
        name="airflow-kpo-test",
        resources={
            "requests": {"memory": "64Mi", "cpu": "250m"},
            "limits": {"memory": "128Mi", "cpu": "500m"}
        },
        do_xcom_push=False,
        retry_delay=pendulum.duration(minutes=5),
    )