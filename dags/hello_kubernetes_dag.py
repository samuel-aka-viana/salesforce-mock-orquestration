from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
        dag_id="test_kubernetes_simple",
        start_date=pendulum.datetime(2025, 9, 14, tz="UTC"),
        catchup=False,
        schedule=None,
) as dag:
    test_task = KubernetesPodOperator(
        task_id="test_simple",
        name="test-simple",
        namespace="airflow",
        image="bash:5.2",
        cmds=["echo", "Hello Kubernetes!"],
        get_logs=True,
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
    )