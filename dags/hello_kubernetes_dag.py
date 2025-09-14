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
    'retries': 0,  # Removido retry para evitar problemas de estado
    'max_active_tis_per_dag': 1,  # Limitar tarefas ativas
}

with DAG(
        dag_id="test_kubernetes_simple",
        default_args=default_args,
        description='Teste simples com KubernetesPodOperator',
        start_date=pendulum.datetime(2025, 9, 14, tz="UTC"),
        catchup=False,
        schedule=None,
        tags=['kubernetes', 'test'],
        # Configurações importantes para Airflow 3.0
        max_active_runs=1,  # Apenas uma execução por vez
        max_active_tasks=1,  # Apenas uma tarefa ativa por vez
) as dag:
    test_task = KubernetesPodOperator(
        task_id="test_simple",
        namespace="airflow",
        service_account_name="airflow",
        image="bash:5.2",
        cmds=["/bin/bash", "-c"],
        arguments=["echo 'Hello Kubernetes from Airflow!' && date && sleep 5 && echo 'Task completed successfully!'"],
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
                "memory": "256Mi"
            }
        },
        restart_policy="Never",
        startup_timeout_seconds=60,
        do_xcom_push=False,
        random_name_suffix=True,
    )