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
    k8s_task = KubernetesPodOperator(
        task_id=f'k8s_download_{i}',
        name=f'download-pod-{i}',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['/bin/sh', '-c'],
        arguments=[
            f'python -c "import urllib.request; print(\'Pod {i} downloading...\'); '
            f'urllib.request.urlretrieve(\'{FILE_URL}\', \'/dev/null\'); '
            f'print(\'Pod {i} Finished\')" '
            '&& echo "Work done. Sleeping 30s for Prometheus..." '
            '&& sleep 30'
        ],
        in_cluster=True,
        dag=dag,
        termination_grace_period=0,
    )
    start_task >> k8s_task >> end_task
