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
        name="hello-world-{{ ds }}-{{ ts_nodash }}",  # Nome único para evitar conflitos
        namespace="airflow",
        image="bash:5.2",
        cmds=["bash"],
        arguments=[
            "-c",
            """
            echo '=========================================='
            echo 'Olá, Mundo, a partir de um Pod Kubernetes! (Airflow 3)'
            echo 'Esta tarefa foi acionada pelo Airflow.'
            echo 'Data/Hora atual:' $(date)
            echo 'Hostname do pod:' $(hostname)
            echo 'Aguardando 5 segundos...'
            sleep 5
            echo 'Tarefa concluída com sucesso!'
            echo '=========================================='

            # Criar um arquivo de teste para verificar se o filesystem funciona
            echo 'Teste de arquivo' > /tmp/teste.txt
            echo 'Conteúdo do arquivo:' $(cat /tmp/teste.txt)

            echo 'RESULTADO: ✅ SUCESSO TOTAL!'
            """
        ],
        # Configurações para funcionar corretamente
        do_xcom_push=False,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=120,
        # Recursos adequados
        container_resources={
            "requests": {"cpu": "100m", "memory": "128Mi"},
            "limits": {"cpu": "300m", "memory": "256Mi"}
        },
        # Labels para identificação
        labels={"app": "airflow-test", "version": "working"},
    )

    end = EmptyOperator(task_id="end")

    start >> hello_kubernetes_task >> end