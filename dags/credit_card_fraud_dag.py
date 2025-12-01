from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

models = ["rf", "lr", "svm", "dt"]  # NOTE: only these are supported in the current implementation

default_args = {
    "owner": "user",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

MINIO_ENDPOINT = "minio.stefan-dev.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "credit-card-fraud-data"
MINIO_SECURE = "false"  # set to true for HTTPS

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
    dag_id="credit_card_fraud",
    default_args=default_args,
    description="DAG for credit card fraud detection",
    schedule=None,
    catchup=False,
) as dag:
    
    preprocessing_task = KubernetesPodOperator(
        task_id="data_preprocessing",
        name="data-preprocessing-pod",
        namespace=NAMESPACE,
        image="kogsi/credit_dag:preprocessing",
        arguments=["--file_name", "creditcard.csv"],
        env_vars=minio_env_dict,
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy="Always",  
        node_selector={"kubernetes.io/hostname": "node1"}, 
    )

    visualization_task = KubernetesPodOperator(
        task_id="data_visualization",
        name="data-visualization-pod",
        namespace=NAMESPACE,
        image="kogsi/credit_dag:visualization",
        arguments=["--file_name", "creditcard.csv"],
        env_vars=minio_env_dict,
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy="Always", 
        node_selector={"kubernetes.io/hostname": "node1"}, 
    )


    training_tasks = []
    for model in models:
        training_task = KubernetesPodOperator(
            task_id=f"model_training_{model}",
            name=f"model-training-{model}",
            namespace=NAMESPACE,
            image="kogsi/credit_dag:training",
            arguments=["--model_type", model],
            env_vars=minio_env_dict,
            is_delete_operator_pod=True,
            get_logs=True,
            image_pull_policy="Always", 
            node_selector={"kubernetes.io/hostname": "node1"},  
        )

        training_tasks.append(training_task)

    
    evaluation_tasks = []
    for model in models:
        evaluation_task = KubernetesPodOperator(
            task_id=f"model_evaluation_{model}",
            name=f"model-evaluation-{model}",
            namespace=NAMESPACE,
            image="kogsi/credit_dag:evaluation",
            arguments=["--model_type", model],
            env_vars=minio_env_dict,
            is_delete_operator_pod=True,
            get_logs=True,
            image_pull_policy="Always", 
            node_selector={"kubernetes.io/hostname": "node1"}, 
        )

        evaluation_tasks.append(evaluation_task)

    
    aggregation_task = KubernetesPodOperator(
        task_id="results_aggregation",
        name="results-aggregation",
        namespace=NAMESPACE,
        image="kogsi/credit_dag:aggregation",
        arguments=["--models", ",".join(models)],  # join into a single string
        env_vars=minio_env_dict,
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy="Always",  
        do_xcom_push=True,
        node_selector={"kubernetes.io/hostname": "node1"}, 
    )

    export_model_task = KubernetesPodOperator(
        task_id="export_model",
        name="export-model",
        namespace=NAMESPACE,
        image="kogsi/credit_dag:export_model",
        env_vars=minio_env_dict,
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy="Always",  
        node_selector={"kubernetes.io/hostname": "node1"}, 
    )

    test_pkl_task = KubernetesPodOperator(
        task_id="test_pkl",
        name="test-pkl-model",
        namespace=NAMESPACE,
        image="kogsi/credit_dag:test_pkl",
        env_vars={
            **minio_env_dict,
            "BEST_MODEL_TYPE": "{{ ti.xcom_pull(task_ids='results_aggregation')['best_model'] }}"
        },
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy="Always",
        node_selector={"kubernetes.io/hostname": "node1"}, 
    )

    test_onnx_task = KubernetesPodOperator(
        task_id="test_onnx",
        name="test-onnx-model",
        namespace=NAMESPACE,
        image="kogsi/credit_dag:test_onnx",
        env_vars= {
            **minio_env_dict,
            "BEST_MODEL_TYPE": "{{ ti.xcom_pull(task_ids='results_aggregation')['best_model'] }}"
        },
        is_delete_operator_pod=True,
        get_logs=True,
        image_pull_policy="Always", 
        node_selector={"kubernetes.io/hostname": "node1"}, 
    )



preprocessing_task >> training_tasks
visualization_task >> training_tasks

# each evaluation task depends only on its respective training task
for model, t_task in zip(models, training_tasks):
    eval_task = next(e for e in evaluation_tasks if e.task_id == f"model_evaluation_{model}")
    t_task >> eval_task

evaluation_tasks >> aggregation_task
aggregation_task >> export_model_task >> [test_pkl_task, test_onnx_task]