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
MINIO_BUCKET = "image-classification-data"

NAMESPACE = "stefan-dev"

minio_env_dict = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_SECURE": "false"
}

with DAG(
    dag_id="image_classification_dag_inference",
    default_args=default_args,
    schedule=None,
    catchup=False,
    start_date=datetime(2025, 1, 1),
) as dag:

    NUM_OFFSET_TASKS = int(Variable.get("image_classification_inference_offset", default_var=1))
    NUM_CROP_TASKS = int(Variable.get("image_classification_inference_crop", default_var=1))
    NUM_ENHANCE_BRIGHTNESS_TASKS = int(Variable.get("image_classification_inference_enhance_brightness", default_var=1))
    NUM_ENHANCE_CONTRAST_TASKS = int(Variable.get("image_classification_inference_enhance_contrast", default_var=1))
    NUM_ROTATE_TASKS = int(Variable.get("image_classification_inference_rotate", default_var=1))
    NUM_GRAYSCALE_TASKS = int(Variable.get("image_classification_inference_grayscale", default_var=1))

    offset_tasks = []
    for i in range(NUM_OFFSET_TASKS):
        offset_task = KubernetesPodOperator(
            task_id=f"offset_task_{i}",
            name=f"offset-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:offset",
            arguments=[
                "--input_image_path", "inference/input",
                "--output_image_path", "inference/offsetted",
                "--dx", "0",
                "--dy", "0",
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_OFFSET_TASKS),
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="Always",
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        offset_tasks.append(offset_task)


    crop_tasks = []
    for i in range(NUM_CROP_TASKS):
        crop_task = KubernetesPodOperator(
            task_id=f"crop_task_{i}",
            name=f"crop-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:crop",
            arguments=[
                "--input_image_path", "inference/offsetted",
                "--output_image_path", "inference/cropped",
                "--left", "20",
                "--top", "20",
                "--right", "330",
                "--bottom", "330",
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_CROP_TASKS),
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="Always",
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        crop_tasks.append(crop_task)

    enhance_brightness_tasks = []
    for i in range(NUM_ENHANCE_BRIGHTNESS_TASKS):
        enhance_brightness_task = KubernetesPodOperator(
            task_id=f"enhance_brightness_task_{i}",
            name=f"enhance-brightness-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:enhance-brightness",
            arguments=[
                "--input_image_path", "inference/cropped",
                "--output_image_path", "inference/enhanced-brightness",
                "--factor", str(1.2),
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_ENHANCE_BRIGHTNESS_TASKS),
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="Always",
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        enhance_brightness_tasks.append(enhance_brightness_task)

    enhance_contrast_tasks = []
    for i in range(NUM_ENHANCE_CONTRAST_TASKS):
        enhance_contrast_task = KubernetesPodOperator(
            task_id=f"enhance_contrast_task_{i}",
            name=f"enhance-contrast-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:enhance-contrast",
            arguments=[
                "--input_image_path", "inference/enhanced-brightness",
                "--output_image_path", "inference/enhanced-contrast",
                "--factor", str(1.2),
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_ENHANCE_BRIGHTNESS_TASKS),
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            image_pull_policy="Always",
            is_delete_operator_pod=True,
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        enhance_contrast_tasks.append(enhance_contrast_task)

    rotate_tasks = []
    for i in range(NUM_ROTATE_TASKS):
        rotate_task = KubernetesPodOperator(
            task_id=f"rotate_task_{i}",
            name=f"rotate-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:rotate",
            arguments=[
                "--input_image_path", "inference/enhanced-contrast",
                "--output_image_path", "inference/rotated",
                "--rotation", " ".join(["0"]),
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_ENHANCE_BRIGHTNESS_TASKS),
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="Always",
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        rotate_tasks.append(rotate_task)

    grayscale_tasks = []
    for i in range(NUM_GRAYSCALE_TASKS):
        grayscale_task = KubernetesPodOperator(
            task_id=f"to_grayscale_task_{i}",
            name=f"to-grayscale-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:to-grayscale",
            arguments=[
                "--input_image_path", "inference/rotated",
                "--output_image_path", "inference/grayscaled",
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_ENHANCE_BRIGHTNESS_TASKS),
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="Always",
            node_selector={"kubernetes.io/hostname": "node1"},
        )
        grayscale_tasks.append(grayscale_task)

    classification_inference_task = KubernetesPodOperator(
        task_id="classification_inference_task",
        name="classification-inference-task",
        namespace=NAMESPACE,
        image="kogsi/image_classification:classification-inference-tf1",
        arguments=[
            "--saved_model_path", "models/",
            "--inference_data_path", "inference/grayscaled",
            "--output_result_path", "inference/results/inference_results.json",
            "--bucket_name", MINIO_BUCKET,
            "--workers", "4",
        ],
        env_vars=minio_env_dict,
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="Always",
        startup_timeout_seconds=600,  # increase time for startup (large image)
        node_selector={"kubernetes.io/hostname": "node1"},
    )

    for crop in crop_tasks:
        offset_tasks >> crop

    for enhance_brightness in enhance_brightness_tasks:
        crop_tasks >> enhance_brightness

    for enhance_contrast in enhance_contrast_tasks:
        enhance_brightness_tasks >> enhance_contrast

    for rotate in rotate_tasks:
        enhance_contrast_tasks >> rotate

    for grayscale in grayscale_tasks:
        rotate_tasks >> grayscale

    grayscale_tasks >> classification_inference_task
