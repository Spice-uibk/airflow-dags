from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'stefanpedratscher',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'example_download_dag',
    default_args=default_args,
    description='Runs 10 parallel pods',
    schedule='@once',
    catchup=False,
    tags=['network_test', 'k8s', 'parallel'],
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

FILE_URL = "https://file-examples.com/wp-content/storage/2017/10/file-example_PDF_1MB.pdf"
for i in range(10):
    # 1. Define the Python script logic
    # We use Triple Quotes (""") to write multi-line python easily.
    # We use {FILE_URL} inside the f-string.
    python_script = f"""
import time
import urllib.request
import os

# --- PART 1: Download & Validate ---
print('Pod {i}: Starting Download...')
dest_file = '/tmp/downloaded_file.pdf'
urllib.request.urlretrieve('{FILE_URL}', dest_file)

# Validation: Check if file exists and has size > 0
if os.path.exists(dest_file):
    size_mb = os.path.getsize(dest_file) / (1024 * 1024)
    print(f'Pod {i}: SUCCESS. File downloaded. Size: {{size_mb:.2f}} MB')
else:
    print(f'Pod {i}: FAILURE. File not found.')
    exit(1) # Fail the pod if download missed

# --- PART 2: CPU Stress (5 Seconds) ---
print('Pod {i}: Spiking CPU for 5 seconds...')
start_time = time.time()
# A tight loop consumes 100% of a single core
while time.time() - start_time < 5:
    _ = 12345 * 67890 

# --- PART 3: Memory Stress (High Usage) ---
# We allocate 200MB of data. 
# NOTE: Ensure your Pod Resource Limit is > 200MB or this will OOMKill!
print('Pod {i}: Spiking Memory (200MB)...')
# Create a byte array of 200MB (200 * 1024 * 1024)
ram_hog = bytearray(200 * 1024 * 1024) 
"""

    flat_script = python_script.replace('\n', '; ')
    k8s_task = KubernetesPodOperator(
        task_id=f'k8s_stress_test_{i}',
        name=f'stress-pod-{i}',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['/bin/sh', '-c'],
        arguments=[f'python -c "{flat_script}"'],
        in_cluster=True,
        dag=dag,
        termination_grace_period=0
    )

    start_task >> k8s_task >> end_task
