from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import json # Needed for the python script inside the pod

default_args = {
    'owner': 'stefanpedratscher',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'hello_world_kubernetes_xcom',
    default_args=default_args,
    description='Dynamic K8s with XCom collection',
    schedule='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hello_world', 'k8s'],
)

with dag:
    start_task = EmptyOperator(task_id='start')

    @task
    def get_input_list():
        # Returns 2 items -> 2 Parallel Pods
        return [[1, 2], [3, 4]]

    input_data = get_input_list()

    # We need a Python script that prints to stdout AND writes to the XCom file
    # This weird string formatting creates a valid Python script to run inside the pod
    def generate_python_cmd(x):
        msg = f"Hello from data {x}!"
        # 1. Print it (so it shows in logs)
        # 2. Dump it to /airflow/xcom/return.json (so Airflow collects it)
        return [f'import json; msg="{msg}"; print(msg); open("/airflow/xcom/return.json", "w").write(json.dumps(msg))']

    k8s_hello_task = KubernetesPodOperator.partial(
        task_id='k8s_hello',
        name='hello-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        in_cluster=True,
        # CRITICAL: This enables the sidecar to read /airflow/xcom/return.json
        do_xcom_push=True,
    ).expand(
        arguments=input_data.map(generate_python_cmd)
    )

    # 3. Fan-In: Receive the results
    # Airflow automatically gathers the parallel outputs into a single list
    @task
    def final_summary(collected_results):
        print("-----------------------------------------")
        print(f"Received {len(collected_results)} results from parallel pods:")
        for res in collected_results:
            print(f" - {res}")
        print("-----------------------------------------")

        # You can now process the combined data here
        return "Summary Complete"

    # Pass the output of the KPO task (.output) to the summary task
    summary_task = final_summary(k8s_hello_task.output)

    end_task = EmptyOperator(task_id='end')

    start_task >> input_data >> k8s_hello_task >> summary_task >> end_task
