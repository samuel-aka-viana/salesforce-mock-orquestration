from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Configuração padrão para todos os operadores no DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

with DAG(
        dag_id="test_kubernetes_simple",
        default_args=default_args,
        description='Teste simples com KubernetesPodOperator',
        start_date=pendulum.datetime(2025, 9, 14, tz="UTC"),
        catchup=False,
        schedule=None,
        tags=['kubernetes', 'test'],
) as dag:
    test_task = KubernetesPodOperator(
        task_id="test_simple",
        namespace="airflow",
        service_account_name="airflow",
        image="bash:5.2",
        cmds=["/bin/bash", "-c"],
        arguments=["echo 'Hello Kubernetes from Airflow!' && sleep 10 && echo 'Task completed successfully!'"],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        labels={"app": "airflow", "test": "kubernetes"},
        resources={
            "requests": {
                "cpu": "100m",
                "memory": "128Mi"
            },
            "limits": {
                "cpu": "500m",
                "memory": "512Mi"
            }
        },
        restart_policy="Never",
        startup_timeout_seconds=120,
    )

    # Teste adicional com Python
    test_python_task = KubernetesPodOperator(
        task_id="test_python",
        namespace="airflow",
        service_account_name="airflow",
        image="python:3.12-slim",
        cmds=["python", "-c"],
        arguments=["""
                    import sys
                    import os
                    print(f"Python version: {sys.version}")
                    print(f"Pod name: {os.environ.get('HOSTNAME', 'unknown')}")
                    print("Python test completed successfully!")
                    """],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        labels={"app": "airflow", "test": "python"},
        resources={
            "requests": {
                "cpu": "100m",
                "memory": "128Mi"
            },
            "limits": {
                "cpu": "500m",
                "memory": "512Mi"
            }
        },
        restart_policy="Never",
        startup_timeout_seconds=120,
    )

    test_task >> test_python_task
