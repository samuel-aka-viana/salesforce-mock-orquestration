from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_kubernetes_executor',
    default_args=default_args,
    description='Teste do KubernetesExecutor e KubernetesPodOperator',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'kubernetes'],
)

def print_hello():
    print("Hello from KubernetesExecutor!")
    return "success"

python_task = PythonOperator(
    task_id='python_task',
    python_callable=print_hello,
    dag=dag,
)

resources = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "128Mi"},
    limits={"cpu": "200m", "memory": "256Mi"}
)

k8s_task = KubernetesPodOperator(
    task_id='kubernetes_pod_task',
    name='test-pod',
    namespace='airflow',
    image='python:3.9-slim',
    cmds=['python', '-c'],
    arguments=['print("Hello from KubernetesPodOperator!"); import time; time.sleep(10)'],
    resources=resources,
    is_delete_operator_pod=True,
    get_logs=True,
    service_account_name='airflow',
    dag=dag,
)

python_task >> k8s_task