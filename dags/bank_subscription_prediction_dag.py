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

MINIO_ENDPOINT = "minio.stefan-dev.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "bank-subscription-prediction-data"
MINIO_SECURE = "false"
DATA_PATH = "https://archive.ics.uci.edu/ml/machine-learning-databases/00222/bank-additional.zip"

NAMESPACE = "stefan-dev"

# Base environment variables
minio_env_dict = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_SECURE": MINIO_SECURE,
    "MINIO_BUCKET": MINIO_BUCKET,
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
        image="kogsi/bank_dag:loading",
        cmds=["python3", "loading.py"],
        arguments=["--data_path", DATA_PATH],
        env_vars=minio_env_dict, 
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        do_xcom_push=True,
        reattach_on_restart=True,
    )

    cleaning_task = KubernetesPodOperator(
        task_id="cleaning",
        name="cleaning",
        namespace=NAMESPACE,
        image="kogsi/bank_dag:cleaning",
        cmds=["python3", "cleaning.py"],
        env_vars={
            **minio_env_dict,
            "LOADING_XCOM": "{{ ti.xcom_pull(task_ids='loading') }}"
        },
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        do_xcom_push=True,
        reattach_on_restart=True,
    )

    preprocessing_task = KubernetesPodOperator(
        task_id="preprocessing",
        name="preprocessing",
        namespace=NAMESPACE,
        image="kogsi/bank_dag:preprocessing",
        cmds=["python3", "preprocessing.py"],
        env_vars={
            **minio_env_dict,
            "CLEANING_XCOM": "{{ ti.xcom_pull(task_ids='cleaning') }}"
        },
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        do_xcom_push=True,
        reattach_on_restart=True,
    )

    splitting_task = KubernetesPodOperator(
        task_id="splitting",
        name="splitting",
        namespace=NAMESPACE,
        image="kogsi/bank_dag:splitting",
        cmds=["python3", "splitting.py"],
        env_vars={
            **minio_env_dict,
            "PREPROCESSING_XCOM": "{{ ti.xcom_pull(task_ids='preprocessing') }}"
        },
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        do_xcom_push=True,
        reattach_on_restart=True,
    )

    training_task = KubernetesPodOperator(
        task_id="training",
        name="training",
        namespace=NAMESPACE,
        image="kogsi/bank_dag:training",
        cmds=["python3", "training.py"],
        env_vars={
            **minio_env_dict,
            "X_TRAIN": "{{ ti.xcom_pull(task_ids='splitting')['X_train'] }}",
            "Y_TRAIN": "{{ ti.xcom_pull(task_ids='splitting')['y_train'] }}"
        },
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        do_xcom_push=True,
        reattach_on_restart=True,
    )

    evaluation_task = KubernetesPodOperator(
        task_id="evaluation",
        name="evaluation",
        namespace=NAMESPACE,
        image="kogsi/bank_dag:evaluation",
        cmds=["python3", "evaluation.py"],
        env_vars={
            **minio_env_dict,
            "MODEL": "{{ ti.xcom_pull(task_ids='training')['model'] }}",
            "FEATURE_SELECTOR": "{{ ti.xcom_pull(task_ids='training')['feature_selector'] }}",
            "X_TEST": "{{ ti.xcom_pull(task_ids='splitting')['X_test'] }}",
            "Y_TEST": "{{ ti.xcom_pull(task_ids='splitting')['y_test'] }}"
        },
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        do_xcom_push=False,
        reattach_on_restart=True,
    )

    loading_task >> cleaning_task >> preprocessing_task >> splitting_task >> training_task >> evaluation_task