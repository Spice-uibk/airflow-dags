from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

default_args = {
    'owner': 'stefanpedratscher',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'hello_world_kubernetes_dynamic',
    default_args=default_args,
    description='A simple Hello World DAG using Kubernetes operator',
    schedule='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hello_world', 'k8s'],
)

with dag:
    start_task = EmptyOperator(task_id='start')

    @task
    def get_input_list():
        count = int(Variable.get("kpo_parallelism_count", default_var=4))
        return [f"Instance {i}" for i in range(count)]

    input_data = get_input_list()

    k8s_hello_task = KubernetesPodOperator.partial(
        task_id='k8s_hello',
        name='hello-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        in_cluster=True,
    ).expand(
        arguments=input_data.map(lambda x: [f'print("Hello from {x}!")'])
    )
    end_task = EmptyOperator(task_id='end')
    start_task >> input_data >> k8s_hello_task >> end_task
