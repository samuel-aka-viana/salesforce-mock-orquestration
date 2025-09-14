from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Configurações de ambiente que serão injetadas no pod
task_env_vars = {
    "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": "http://airflow-api-server.airflow.svc.cluster.local:8080/execution/",
    "AIRFLOW__KUBERNETES__NAMESPACE": "airflow",
    "AIRFLOW__KUBERNETES__IN_CLUSTER": "True",
    "AIRFLOW__EXECUTION__TASK_EXECUTION_ENABLED": "True",
    "AIRFLOW__API__AUTH_BACKENDS": "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
}

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
        name="hello-world-{{ ds }}-{{ ts_nodash }}",
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
        # FORÇAR variáveis de ambiente diretamente no pod
        env_vars=task_env_vars,
        do_xcom_push=False,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        # Configuração adicional de recursos
        resources={
            "request_cpu": "100m",
            "request_memory": "128Mi",
            "limit_cpu": "500m",
            "limit_memory": "512Mi"
        },
    )

    end = EmptyOperator(task_id="end")

    start >> hello_kubernetes_task >> end