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
    'salesforce_kubernetes_dag',
    default_args=default_args,
    description='DAG de Orquestração Salesforce - Airflow 3.0.2',
    schedule=None,
    catchup=False,
    tags=['salesforce', 'kubernetes', 'v3.0.2'],
    max_active_runs=1,
    max_active_tasks=3,
)

def hello_python():
    """Função Python de validação inicial"""
    print("=== Salesforce Airflow DAG Iniciada ===")
    print("Validando conexões e configurações...")
    return "validation_success"

def process_salesforce_data():
    """Simulação de processamento de dados Salesforce"""
    print("=== Processando dados Salesforce ===")
    print("Conectando com APIs...")
    print("Processamento concluído com sucesso!")
    return "processing_success"

validation_task = PythonOperator(
    task_id='validate_environment',
    python_callable=hello_python,
    dag=dag,
)

salesforce_task = PythonOperator(
    task_id='process_salesforce_data',
    python_callable=process_salesforce_data,
    dag=dag,
)

k8s_processing_task = KubernetesPodOperator(
    task_id='heavy_processing_k8s',
    name='salesforce-processing-pod',
    namespace='airflow',
    image='python:3.12-slim',
    cmds=['python', '-c'],
    arguments=['''
import json
import time

print("=== Iniciando processamento pesado no Kubernetes ===")
print("Simulando transformação de dados...")

# Simula processamento
for i in range(5):
    print(f"Processando lote {i+1}/5...")
    time.sleep(2)

print("=== Processamento concluído com sucesso! ===")
print(json.dumps({"status": "success", "processed_records": 1000}))
    '''],
    is_delete_operator_pod=True,
    get_logs=True,
    startup_timeout_seconds=120,
    do_xcom_push=False,
    # Configurações específicas para Airflow 3.0.2
    resources={
        'requests': {'cpu': '100m', 'memory': '256Mi'},
        'limits': {'cpu': '500m', 'memory': '512Mi'}
    },
    dag=dag,
)

# Task 4: Validação final
final_validation_task = KubernetesPodOperator(
    task_id='final_validation',
    name='validation-pod',
    namespace='airflow',
    image='python:3.12-slim',
    cmds=['python', '-c'],
    arguments=['''
print("=== Validação Final ===")
print("Verificando resultados do processamento...")
print("Pipeline executado com sucesso!")
print("Status: COMPLETED")
    '''],
    is_delete_operator_pod=True,
    get_logs=True,
    startup_timeout_seconds=60,
    do_xcom_push=False,
    dag=dag,
)

# Definindo as dependências do fluxo
validation_task >> salesforce_task >> k8s_processing_task >> final_validation_task