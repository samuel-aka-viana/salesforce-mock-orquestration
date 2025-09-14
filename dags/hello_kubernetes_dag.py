from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="hello_kubernetes_world_working",
    start_date=pendulum.datetime(2025, 9, 14, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["kubernetes", "working"],
    description="Versão funcional do Hello World Kubernetes",
    max_active_runs=1,
) as dag:
    start = EmptyOperator(task_id="start")

    hello_kubernetes_task = KubernetesPodOperator(
        task_id="hello_kubernetes_pod_task",
        name="hello-world",
        random_name_suffix=True,
        namespace="airflow",
        image="bash:5.2",
        cmds=["bash"],
        arguments=[
            "-c",
            """
            echo '=========================================='
            echo 'Olá, Mundo, a partir de um Pod Kubernetes! (Airflow)'
            echo 'Esta tarefa foi acionada pelo Airflow.'
            echo 'Data/Hora atual:' $(date)
            echo 'Hostname do pod:' $(hostname)
            echo 'Aguardando 5 segundos...'
            sleep 5
            echo 'Tarefa concluída com sucesso!'
            echo '=========================================='
            echo 'Teste de arquivo' > /tmp/teste.txt
            echo 'Conteúdo do arquivo:' $(cat /tmp/teste.txt)
            echo 'RESULTADO: SUCESSO TOTAL'
            """
        ],
        env_vars={
            "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": "http://airflow-api-server.airflow.svc.cluster.local:8080/execution/",
            "AIRFLOW__KUBERNETES__NAMESPACE": "airflow",
            "AIRFLOW__KUBERNETES__IN_CLUSTER": "True",
            "AIRFLOW__EXECUTION__TASK_EXECUTION_ENABLED": "True",
            "JOB_LABEL": "hello-world",
        },
        get_logs=True,
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,
        container_resources={
            "requests": {
                "cpu": "100m",
                "memory": "128Mi"
            },
            "limits": {
                "cpu": "500m",
                "memory": "512Mi"
            }
        },
        labels={
            "airflow_task": "hello_kubernetes_pod_task",
            "airflow_dag": "hello_kubernetes_world_working",
        },
    )

    end = EmptyOperator(task_id="end")

    start >> hello_kubernetes_task >> end