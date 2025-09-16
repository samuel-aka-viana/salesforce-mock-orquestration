from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
dag = DAG(
    'minimal_kubernetes_dag',
    default_args=default_args,
    description='DAG Mínima e Garantida para Airflow 3.0.2',
    schedule=None,
    catchup=False,
    tags=['minimal', 'kubernetes', 'v3.0.2'],
)

def hello_python():
    """Função Python simples"""
    print("Hello from Python Task!")
    return "python_success"

# Task Python
python_task = PythonOperator(
    task_id='hello_python',
    python_callable=hello_python,
    dag=dag,
)

# Task Kubernetes (Versão Mínima)
k8s_task = KubernetesPodOperator(
    task_id='hello_kubernetes',
    name='hello-k8s-pod',
    namespace='airflow',
    image='python:3.12-slim',
    cmds=['python', '-c'],
    arguments=['print("Hello from Kubernetes Pod!"); print("Task completed successfully!")'],
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Dependências
python_task >> k8s_task