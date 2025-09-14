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
    max_active_runs=1,  # remova se sua versão reclamar
) as dag:
    start = EmptyOperator(task_id="start")

    hello_kubernetes_task = KubernetesPodOperator(
        task_id="hello_kubernetes_pod_task",
        # Evite Jinja e caracteres inválidos no nome
        name="hello-world",
        random_name_suffix=True,  # garante unicidade e comprimento válido
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
        # Removido AIRFLOW__* desnecessário. Se quiser manter, saiba que não afetam o bash.
        env_vars={
            # exemplo útil: rastreio básico
            "JOB_LABEL": "hello-world",
        },
        get_logs=True,
        do_xcom_push=False,
        is_delete_operator_pod=True,
        in_cluster=True,  # mantenha apenas este
        resources={
            "request_cpu": "100m",
            "request_memory": "128Mi",
            "limit_cpu": "500m",
            "limit_memory": "512Mi",
        },
        labels={
            "airflow_task": "hello_kubernetes_pod_task",
            "airflow_dag": "hello_kubernetes_world_working",
        },
        # restart_policy é "Never" por padrão; ajuste se precisar
    )

    end = EmptyOperator(task_id="end")

    start >> hello_kubernetes_task >> end
