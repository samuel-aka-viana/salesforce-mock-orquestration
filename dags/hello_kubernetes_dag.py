from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator

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
    'minimal_working_dag',
    default_args=default_args,
    description='DAG Mínima e Funcional - Airflow 3.0.2',
    schedule=None,
    catchup=False,
    tags=['minimal', 'kubernetes', 'working'],
    max_active_runs=1,
)

def hello_world():
    """Função Python simples"""
    print("=== Hello World DAG ===")
    print("Task Python executada com sucesso!")
    return "success"

# Task 1: Validação Python simples
python_task = PythonOperator(
    task_id='hello_python',
    python_callable=hello_world,
    dag=dag,
)

# Task 2: Pod Kubernetes SIMPLES (sem resources especificados)
k8s_simple_task = KubernetesPodOperator(
    task_id='hello_kubernetes',
    name='hello-k8s-pod',
    namespace='airflow',
    image='python:3.12-slim',
    cmds=['python', '-c'],
    arguments=['print("Hello from Kubernetes!"); print("Task completed successfully!")'],
    is_delete_operator_pod=True,
    get_logs=True,
    startup_timeout_seconds=120,
    do_xcom_push=False,
    dag=dag,
)

# Dependência simples
python_task >> k8s_simple_task