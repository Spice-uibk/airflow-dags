from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
import json

default_args = {
    'owner': 'stefanpedratscher',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kpo_only_dynamic_mapping',
    default_args=default_args,
    description='Dynamic mapping using ONLY KubernetesPodOperators',
    schedule='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['k8s', 'xcom'],
)

with dag:
    generator_cmd = """
import json
import os
import time
time.sleep(20)
count = int(os.environ.get("COUNT", 4))
data = [f"Instance {i}" for i in range(count)]
print(f"Generating list: {data}")
with open('/airflow/xcom/return.json', 'w') as f:
    json.dump(data, f)
    """

    generator_pod = KubernetesPodOperator(
        task_id='generate_list_pod',
        name='generator-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        env_vars={'COUNT': '{{ var.value.get("kpo_parallelism_count", 4) }}'},
        cmds=['python', '-c', generator_cmd],
        in_cluster=True,
        do_xcom_push=True,
        node_selector={"kubernetes.io/hostname": "node1"},
    )

    consumer_pods = KubernetesPodOperator.partial(
        task_id='k8s_hello',
        name='hello-pod',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        in_cluster=True,
        node_selector={"kubernetes.io/hostname": "node1"},
    ).expand(
        arguments=generator_pod.output.map(
            lambda x: [f'import time; print("Sleeping..."); time.sleep(20); print("Hello from {x}!")']
        )
    )
    generator_pod >> consumer_pods
