from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    "owner": 'user',
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# MinIO configuration
MINIO_ENDPOINT = "minio.stefan-dev.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "cats-and-dogs"

NAMESPACE = "stefan-dev"

minio_env_dict = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_SECURE": "false"
}

with DAG(
    dag_id="cats_dogs_classification_dag_training",
    default_args=default_args,
    schedule=None,
    catchup=False,
    start_date=datetime(2025, 1, 1),
) as dag:

    NUM_PARALLEL_TASKS = int(Variable.get("cats_dogs_classification_training_num_workers", default_var=1))

    offset_tasks = []
    for i in range(NUM_PARALLEL_TASKS):
        offset_task = KubernetesPodOperator(
            task_id=f"offset_task_{i}",
            name=f"offset-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:offset",
            arguments=[
                "--input_image_path", "training/input",
                "--output_image_path", f"training/offsetted/{i}",
                "--dx", "0",
                "--dy", "0",
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_PARALLEL_TASKS),
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        offset_tasks.append(offset_task)


    crop_tasks = []
    for i in range(NUM_PARALLEL_TASKS):
        crop_task = KubernetesPodOperator(
            task_id=f"crop_task_{i}",
            name=f"crop-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:crop",
            arguments=[
                "--input_image_path", f"training/offsetted/{i}",
                "--output_image_path", f"training/cropped/{i}",
                "--left", "20",
                "--top", "20",
                "--right", "330",
                "--bottom", "330",
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", "0",
                "--num_tasks", "1",
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        crop_tasks.append(crop_task)

    enhance_brightness_tasks = []
    for i in range(NUM_PARALLEL_TASKS):
        enhance_brightness_task = KubernetesPodOperator(
            task_id=f"enhance_brightness_task_{i}",
            name=f"enhance-brightness-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:enhance-brightness",
            arguments=[
                "--input_image_path", f"training/cropped/{i}",
                "--output_image_path", f"training/enhanced-brightness/{i}",
                "--factor", str(1.2),
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", "0",
                "--num_tasks", "1",
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        enhance_brightness_tasks.append(enhance_brightness_task)

    enhance_contrast_tasks = []
    for i in range(NUM_PARALLEL_TASKS):
        enhance_contrast_task = KubernetesPodOperator(
            task_id=f"enhance_contrast_task_{i}",
            name=f"enhance-contrast-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:enhance-contrast",
            arguments=[
                "--input_image_path", f"training/enhanced-brightness/{i}",
                "--output_image_path", f"training/enhanced-contrast/{i}",
                "--factor", str(1.2),
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", "0",
                "--num_tasks", "1",
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            image_pull_policy="IfNotPresent",
            is_delete_operator_pod=True,
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        enhance_contrast_tasks.append(enhance_contrast_task)

    rotate_tasks = []
    for i in range(NUM_PARALLEL_TASKS):
        rotate_task = KubernetesPodOperator(
            task_id=f"rotate_task_{i}",
            name=f"rotate-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:rotate",
            arguments=[
                "--input_image_path", f"training/enhanced-contrast/{i}",
                "--output_image_path", f"training/rotated/{i}",
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", "0",
                "--num_tasks", "1",
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        rotate_tasks.append(rotate_task)

    grayscale_tasks = []
    for i in range(NUM_PARALLEL_TASKS):
        grayscale_task = KubernetesPodOperator(
            task_id=f"to_grayscale_task_{i}",
            name=f"to-grayscale-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:to-grayscale",
            arguments=[
                "--input_image_path", f"training/rotated/{i}",
                "--output_image_path", "training/grayscaled",
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", "0",
                "--num_tasks", "1",
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        grayscale_tasks.append(grayscale_task)

    classification_inference_task = KubernetesPodOperator(
        task_id="classification_inference_task_training",
        name="classification-inference-task-training",
        namespace=NAMESPACE,
        image="kogsi/image_classification:classification-train-tf1",
        arguments=[
            "--train_data_path", "training/grayscaled",
            "--output_artifact_path", "models/",
            "--bucket_name", MINIO_BUCKET,
            "--validation_split", "0.2",
            # "--validation_data_path", "training/validation",
            "--epochs", "5",
            "--batch_size", "32",
            "--early_stop_patience", "5",
            "--dropout_rate", "0.2",
            "--image_size", "128 128",
            "--num_layers", "3",
            "--filters_per_layer", "32 32 32",
            "--kernel_sizes", "3 3 3",
            "--workers", "4",
        ],
        env_vars=minio_env_dict,
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        startup_timeout_seconds=600,  # increase time for startup (large image)
        node_selector={"kubernetes.io/hostname": "node1"},
    )

    for i in range(NUM_PARALLEL_TASKS):
        (
                offset_tasks[i]
                >> crop_tasks[i]
                >> enhance_brightness_tasks[i]
                >> enhance_contrast_tasks[i]
                >> rotate_tasks[i]
                >> grayscale_tasks[i]
        )

    grayscale_tasks >> classification_inference_task