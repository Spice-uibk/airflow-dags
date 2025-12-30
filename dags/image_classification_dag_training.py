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
    dag_id="image_classification_dag_training",
    default_args=default_args,
    schedule=None,
    catchup=False,
    start_date=datetime(2025, 1, 1),
) as dag:

    NUM_OFFSET_TASKS = int(Variable.get("image_classification_training_offset", default_var=2))
    NUM_CROP_TASKS = int(Variable.get("image_classification_training_crop", default_var=2))
    NUM_ENHANCE_BRIGHTNESS_TASKS = int(Variable.get("image_classification_training_enhance_brightness", default_var=2))
    NUM_ENHANCE_CONTRAST_TASKS = int(Variable.get("image_classification_training_enhance_contrast", default_var=2))
    NUM_ROTATE_TASKS = int(Variable.get("image_classification_training_rotate", default_var=2))
    NUM_GRAYSCALE_TASKS = int(Variable.get("image_classification_training_grayscale", default_var=2))

    offset_tasks = []
    for i in range(NUM_OFFSET_TASKS):
        offset_task = KubernetesPodOperator(
            task_id=f"offset_task_{i}",
            name=f"offset-task-{i}",
            namespace=NAMESPACE,
            image="kogsi/image_classification:offset",
            cmds=["python3", "offset.py"],
            arguments=[
                "--input_image_path", "training/input",
                "--output_image_path", "training/offsetted",
                "--dx", "0",
                "--dy", "0",
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_OFFSET_TASKS),
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
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
            cmds=["python3", "crop.py"],
            arguments=[
                "--input_image_path", "training/offsetted",
                "--output_image_path", "training/cropped",
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
            image_pull_policy="IfNotPresent",
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
            cmds=["python3", "enhance-brightness.py"],
            arguments=[
                "--input_image_path", "training/cropped",
                "--output_image_path", "training/enhanced-brightness",
                "--factor", str(1.2),
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_ENHANCE_BRIGHTNESS_TASKS),
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
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
            cmds=["python3", "enhance-contrast.py"],
            arguments=[
                "--input_image_path", "training/enhanced-brightness",
                "--output_image_path", "training/enhanced-contrast",
                "--factor", str(1.2),
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_ENHANCE_BRIGHTNESS_TASKS),
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            image_pull_policy="IfNotPresent",
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
                "--input_image_path", "training/enhanced-contrast",
                "--output_image_path", "training/rotated",
                "--rotation", " ".join(["90", "270", "0", "180"]),
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_ENHANCE_BRIGHTNESS_TASKS),
            ],
            cmds=["python3", "rotate.py"],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
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
            cmds=["python3", "to-grayscale.py"],
            arguments=[
                "--input_image_path", "training/rotated",
                "--output_image_path", "training/grayscaled",
                "--bucket_name", MINIO_BUCKET,
                "--chunk_id", str(i),
                "--num_tasks", str(NUM_ENHANCE_BRIGHTNESS_TASKS),
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
        image="kogsi/image_classification:classification-train",
        cmds=["python3", "classification-train.py"],
        arguments=[
            "--train_data_path", "training/grayscaled",
            "--output_artifact_path", "models/",
            "--bucket_name", MINIO_BUCKET,
            "--validation_split", "0.2",
            # "--validation_data_path", "inference/validation_data",
            "--epochs", "10",
            "--batch_size", "32",
            "--early_stop_patience", "5",
            "--dropout_rate", "0.2",
            "--image_size", "256 256",
            "--num_layers", "3",
            "--filters_per_layer", "64 64 64",
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