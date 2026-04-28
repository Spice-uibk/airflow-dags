from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from datetime import datetime, timedelta

default_args = {
    "owner": 'user',
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

shared_secrets = [
    Secret("env", "ONEDATA_HOST", "predictor-secrets", "onedata-host"),
    Secret("env", "ONEDATA_TOKEN", "predictor-secrets", "token-provider"),
    Secret("env", "ONEDATA_SPACE", "predictor-secrets", "onedata-space"),
    Secret("env", "OTLP_ENDPOINT", "predictor-secrets", "otlp-endpoint"),
]

loading_secret = Secret("env", "DATA_PATH", "predictor-secrets", "data-path")

NAMESPACE = "default"

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
        image="leichtabgelenkt/bank_subscription_prediction:loading",
        cmds=["/bin/sh", "-c"],
        arguments=['python3 loading.py --data_path "$DATA_PATH"'],
        secrets=shared_secrets + [loading_secret],
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
        image="leichtabgelenkt/bank_subscription_prediction:cleaning",
        cmds=["python3", "cleaning.py"],
        secrets=shared_secrets,
        env_vars={
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
        image="leichtabgelenkt/bank_subscription_prediction:preprocessing",
        cmds=["python3", "preprocessing.py"],
        secrets=shared_secrets,
        env_vars={
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
        image="leichtabgelenkt/bank_subscription_prediction:splitting",
        cmds=["python3", "splitting.py"],
        secrets=shared_secrets,
        env_vars={
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
        image="leichtabgelenkt/bank_subscription_prediction:training",
        cmds=["python3", "training.py"],
        secrets=shared_secrets,
        env_vars={
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
        image="leichtabgelenkt/bank_subscription_prediction:evaluation",
        cmds=["python3", "evaluation.py"],
        secrets=shared_secrets,
        env_vars={
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