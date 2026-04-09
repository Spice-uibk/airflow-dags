from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    "owner": 'user',
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Onedata configuration
ONEDATA_HOST = "https://data.spice-platform.eu"
ONEDATA_TOKEN = "MDAyNGxvY2F00aW9uIGRhdGEuc3BpY2UtcGxhdGZvcm00uZXUKMDA2YmlkZW500aWZpZXIgMi9ubWQvdXNyLTVhZWEzZThiNTgwMDYzZDRhOWExOWY3NWFjMzViMWYzY2g5NzJlL2FjdC82YzU00YTc3ZTRjMjA3NTJkNzYwOTFiNWNlNGU00MzZmYWNoNTc3OAowMDE5Y2lkIGludGVyZmFjZSA9IHJlc3QKMDAxOGNpZCBzZXJ2aWNlID00gb3B3LSoKMDAyZnNpZ25hdHVyZSD9xyoau4ixjkpymKgfEXCok3QvWlqsxElGKjVRxVf32wo"
ONEDATA_SPACE = "LucasSpace"
DATA_PATH = "https://archive.ics.uci.edu/ml/machine-learning-databases/00222/bank-additional.zip"

NAMESPACE = "default"

# Base environment variables
onedata_env_dict = {
    "ONEDATA_HOST": ONEDATA_HOST,
    "ONEDATA_TOKEN": ONEDATA_TOKEN,
    "ONEDATA_SPACE": ONEDATA_SPACE,
}

with DAG(
    dag_id="bank_subscription_prediction",
    default_args=default_args,
    description="Bank Subscription Prediction",
    schedule=None,
    catchup=False,
) as dag:
    
    loading_task = KubernetesPodOperator(
        task_id="loading",
        name="loading",
        namespace=NAMESPACE,
        image="leichtabgelenkt/bank_dag:loading",
        cmds=["python3", "loading.py"],
        arguments=["--data_path", DATA_PATH],
        env_vars=onedata_env_dict, 
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="Always",
        do_xcom_push=True,
        reattach_on_restart=True,
        # node_selector={"kubernetes.io/hostname": "node1"}, 
    )

    cleaning_task = KubernetesPodOperator(
        task_id="cleaning",
        name="cleaning",
        namespace=NAMESPACE,
        image="leichtabgelenkt/bank_dag:cleaning",
        cmds=["python3", "cleaning.py"],
        env_vars={
            **onedata_env_dict,
            "LOADING_XCOM": "{{ ti.xcom_pull(task_ids='loading') }}"
        },
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="Always",
        do_xcom_push=True,
        reattach_on_restart=True,
        # node_selector={"kubernetes.io/hostname": "node1"}, 
    )

    preprocessing_task = KubernetesPodOperator(
        task_id="preprocessing",
        name="preprocessing",
        namespace=NAMESPACE,
        image="leichtabgelenkt/bank_dag:preprocessing",
        cmds=["python3", "preprocessing.py"],
        env_vars={
            **onedata_env_dict,
            "CLEANING_XCOM": "{{ ti.xcom_pull(task_ids='cleaning') }}"
        },
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="Always",
        do_xcom_push=True,
        reattach_on_restart=True,
        # node_selector={"kubernetes.io/hostname": "node1"}, 
    )

    splitting_task = KubernetesPodOperator(
        task_id="splitting",
        name="splitting",
        namespace=NAMESPACE,
        image="leichtabgelenkt/bank_dag:splitting",
        cmds=["python3", "splitting.py"],
        env_vars={
            **onedata_env_dict,
            "PREPROCESSING_XCOM": "{{ ti.xcom_pull(task_ids='preprocessing') }}"
        },
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="Always",
        do_xcom_push=True,
        reattach_on_restart=True,
        # node_selector={"kubernetes.io/hostname": "node1"}, 
    )

    training_task = KubernetesPodOperator(
        task_id="training",
        name="training",
        namespace=NAMESPACE,
        image="leichtabgelenkt/bank_dag:training",
        cmds=["python3", "training.py"],
        env_vars={
            **onedata_env_dict,
            "X_TRAIN": "{{ ti.xcom_pull(task_ids='splitting')['X_train'] }}",
            "Y_TRAIN": "{{ ti.xcom_pull(task_ids='splitting')['y_train'] }}"
        },
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="Always",
        do_xcom_push=True,
        reattach_on_restart=True,
        # node_selector={"kubernetes.io/hostname": "node1"}, 
    )

    evaluation_task = KubernetesPodOperator(
        task_id="evaluation",
        name="evaluation",
        namespace=NAMESPACE,
        image="leichtabgelenkt/bank_dag:evaluation",
        cmds=["python3", "evaluation.py"],
        env_vars={
            **onedata_env_dict,
            "MODEL": "{{ ti.xcom_pull(task_ids='training')['model'] }}",
            "FEATURE_SELECTOR": "{{ ti.xcom_pull(task_ids='training')['feature_selector'] }}",
            "X_TEST": "{{ ti.xcom_pull(task_ids='splitting')['X_test'] }}",
            "Y_TEST": "{{ ti.xcom_pull(task_ids='splitting')['y_test'] }}"
        },
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="Always",
        do_xcom_push=False,
        reattach_on_restart=True,
        # node_selector={"kubernetes.io/hostname": "node1"}, 
    )

    loading_task >> cleaning_task >> preprocessing_task >> splitting_task >> training_task >> evaluation_task