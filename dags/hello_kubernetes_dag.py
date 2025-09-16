from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s

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

# Definição da DAG - usando 'schedule' em vez de 'schedule_interval'
dag = DAG(
    'test_kubernetes_executor_v3',
    default_args=default_args,
    description='Teste do KubernetesExecutor e KubernetesPodOperator - Airflow 3.0.2',
    schedule=None,  # Mudança: schedule em vez de schedule_interval
    catchup=False,
    tags=['test', 'kubernetes', 'v3.0'],
    max_active_runs=1,
    max_active_tasks=10,
)

def print_hello(**context):
    """Função Python simples para teste"""
    print("Hello from KubernetesExecutor!")
    print(f"Execution date: {context['ds']}")
    print(f"Task instance: {context['task_instance']}")
    return "success"

# Task Python usando PythonOperator
python_task = PythonOperator(
    task_id='python_task',
    python_callable=print_hello,
    dag=dag,
)

# Configuração de recursos para o pod Kubernetes
pod_resources = k8s.V1ResourceRequirements(
    requests={
        "cpu": "100m",
        "memory": "128Mi"
    },
    limits={
        "cpu": "500m",
        "memory": "512Mi"
    }
)

# Configuração de security context para o container
container_security_context = k8s.V1SecurityContext(
    run_as_non_root=True,
    run_as_user=1000,
    run_as_group=1000,
    allow_privilege_escalation=False,
    read_only_root_filesystem=False,
    capabilities=k8s.V1Capabilities(drop=["ALL"]),
)

# Task Kubernetes usando KubernetesPodOperator
k8s_task = KubernetesPodOperator(
    task_id='kubernetes_pod_task',
    name='test-pod-v3',
    namespace='airflow',
    image='python:3.12-slim',  # Versão mais recente do Python
    cmds=['python', '-c'],
    arguments=[
        '''
import time
import os
print("Hello from KubernetesPodOperator!")
print(f"Pod name: {os.environ.get('HOSTNAME', 'unknown')}")
print("Starting processing...")
time.sleep(10)
print("Processing completed successfully!")
        '''
    ],
    container_resources=pod_resources,  # Correto para v3.0.2
    container_security_context=container_security_context,  # Correto para v3.0.2
    is_delete_operator_pod=True,
    get_logs=True,
    service_account_name='airflow',
    dag=dag,
    # Configurações adicionais para v3.0
    startup_timeout_seconds=120,
    env_vars={
        'PYTHONUNBUFFERED': '1',
        'AIRFLOW_VERSION': '3.0.2',
    },
)

# Task adicional para demonstrar paralelismo
def analyze_results(**context):
    """Analisa os resultados das tasks anteriores"""
    print("Analyzing results from previous tasks...")
    print("All tasks completed successfully!")
    return {"status": "completed", "timestamp": str(datetime.now())}

analysis_task = PythonOperator(
    task_id='analysis_task',
    python_callable=analyze_results,
    dag=dag,
)

# Definição das dependências
python_task >> k8s_task >> analysis_task