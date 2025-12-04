import base64
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
    'stress_test_metrics',
    default_args=default_args,
    description='Validates download and runs CPU/Memory stress test',
    schedule='@once',
    catchup=False,
    tags=['stress_test', 'k8s'],
)

start_task = EmptyOperator(task_id='start', dag=dag)
end_task = EmptyOperator(task_id='end', dag=dag)

FILE_URL = "https://freetestdata.com/wp-content/uploads/2021/09/Free_Test_Data_2MB_PDF.pdf"

for i in range(10):
    python_source_code = f"""
import time
import urllib.request
import os
import sys

# --- PART 1: Download & Validate ---
print("Pod {i}: Starting Download...")
dest_file = "/tmp/downloaded_file.pdf"

try:
    urllib.request.urlretrieve("{FILE_URL}", dest_file)
except Exception as e:
    print(f"Pod {i}: Download failed: {{e}}")
    sys.exit(1)

if os.path.exists(dest_file):
    size_mb = os.path.getsize(dest_file) / (1024 * 1024)
    print(f"Pod {i}: SUCCESS. File size: {{size_mb:.2f}} MB")
    if size_mb < 0.1:
        print("Pod {i}: FAILURE. File too small.")
        sys.exit(1)
else:
    print("Pod {i}: FAILURE. File not found.")
    sys.exit(1)

# --- PART 2: CPU Stress (5 Seconds) ---
print("Pod {i}: Spiking CPU (100%) for 5 seconds...")
start_time = time.time()
while time.time() - start_time < 5:
    _ = 12345 * 67890 

# --- PART 3: Memory Stress (Alloc & Hold) ---
print("Pod {i}: Spiking Memory (200MB)...")
# Allocate 200MB
ram_hog = bytearray(200 * 1024 * 1024)

# We must sleep while holding the variable so cAdvisor sees it.
# If we exit immediately, memory is freed before Prometheus scrapes.
print("Pod {i}: Holding memory and sleeping 30s for Prometheus scrape...")
time.sleep(30)

print("Pod {i}: Done.")
"""

    script_b64 = base64.b64encode(python_source_code.encode('utf-8')).decode('utf-8')
    k8s_task = KubernetesPodOperator(
        task_id=f'k8s_stress_{i}',
        name=f'stress-pod-{i}',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['/bin/sh', '-c'],
        arguments=[f'echo {script_b64} | base64 -d | python'],
        in_cluster=True,
        dag=dag,
        termination_grace_period=0,
    )

    start_task >> k8s_task >> end_task
