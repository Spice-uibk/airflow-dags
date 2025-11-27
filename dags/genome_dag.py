from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    "owner": 'user',
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# MinIO configuration
MINIO_ENDPOINT = "minio.stefan-dev.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
CHROM_NR = "22"
MINIO_BUCKET = "genome-data"
KEY_INPUT_INDIVIDUAL = "ALL.chr22.80000.vcf.gz"
KEY_INPUT_SIFTING = "ALL.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.sites.annotation.vcf.gz"

NAMESPACE = "stefan-dev"


# Use a standard dictionary instead of k8s.V1EnvVar objects to save import time
minio_env_dict = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_SECURE": "false"
}

with DAG(
    dag_id='genome_data_processing',
    default_args=default_args,
    description='Genome processing pipeline using KubernetesPodOperator',
    schedule=None,
    catchup=False,
    tags=['genome', 'kubernetes', 'minio'],
    max_active_tasks=16,
) as dag:
    
    # Individual task
     individual_tasks = []
     for x in range(0, 5):  # maybe change constant and change step size accordingly
         counter = x * 2000 + 1
         stop = (x + 1) * 2000 + 1
         
         task = KubernetesPodOperator(
            task_id=f"individual_{x}",
            name=f"individual-{x}",
            namespace=NAMESPACE,
            image="kogsi/genome_dag:individual", 
            cmds=["python3", "individual.py"],
            arguments=[
                "--key_input", KEY_INPUT_INDIVIDUAL,
                "--counter", str(counter),
                "--stop", str(stop),
                "--chromNr", CHROM_NR,
                "--bucket_name", MINIO_BUCKET
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="Always",
            execution_timeout=timedelta(hours=1),
            node_selector={"kubernetes.io/hostname": "node4"}, 
         )
         individual_tasks.append(task)

            
    # Sifting task
     sifting_task = KubernetesPodOperator(
        task_id="sifting",
        name="sifting",
        namespace=NAMESPACE,
        image="kogsi/genome_dag:sifting", 
        cmds=["python3", "sifting.py"],
        arguments=[
            "--key_datafile", KEY_INPUT_SIFTING,
            "--chromNr", CHROM_NR,
            "--bucket_name", MINIO_BUCKET
        ],
        env_vars=minio_env_dict,
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="Always",
        execution_timeout=timedelta(hours=1),
        node_selector={"kubernetes.io/hostname": "node4"}, 
     )
    
     # Individuals merge task
     individuals_merge_task = KubernetesPodOperator(
        task_id="individuals_merge",
        name="individuals-merge",
        namespace=NAMESPACE,
        image="kogsi/genome_dag:individuals-merge",  
        cmds=["python3", "individuals-merge.py"],
        arguments=[
            "--chromNr", CHROM_NR,
            "--keys", ','.join([f'chr22n-{x*2000+1}-{(x+1)*2000+1}.tar.gz' for x in range(5)]),  # take same step size and interation limit as in first loop
            "--bucket_name", MINIO_BUCKET
        ],
        env_vars=minio_env_dict,
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="Always",
        execution_timeout=timedelta(hours=1),
        node_selector={"kubernetes.io/hostname": "node4"}, 
     )
         
         
     # Mutations Overlap task
     mutations_overlap_tasks = []
     pop_arr = ["EUR", "AFR", "EAS", "ALL", "GBR", "SAS", "AMR"]
     
     for pop in pop_arr:
        task = KubernetesPodOperator(
            task_id=f"mutations_overlap_{pop}",
            name=f"mutations-overlap-{pop.lower()}",
            namespace=NAMESPACE,
            image="kogsi/genome_dag:mutations-overlap", 
            cmds=["python3", "mutations-overlap.py"],
            arguments=[
                "--chromNr", CHROM_NR,
                "--POP", pop,
                "--bucket_name", MINIO_BUCKET
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="Always",
            execution_timeout=timedelta(hours=1),
            node_selector={"kubernetes.io/hostname": "node4"}, 
        )
        mutations_overlap_tasks.append(task)
         
         
     # Frequency tasks
     frequency_tasks = []
     for pop in pop_arr:
        task = KubernetesPodOperator(
            task_id=f"frequency_{pop}",
            name=f"frequency-{pop.lower()}",
            namespace=NAMESPACE,
            image="kogsi/genome_dag:frequency", 
            cmds=["python3", "frequency.py"],
            arguments=[
                "--chromNr", CHROM_NR,
                "--POP", pop,
                "--bucket_name", MINIO_BUCKET
            ],
            env_vars=minio_env_dict,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="Always",
            execution_timeout=timedelta(hours=1),
            node_selector={"kubernetes.io/hostname": "node4"}, 
        )
        frequency_tasks.append(task)
         
         
     # Task dependencies
     individual_tasks >> individuals_merge_task
     individuals_merge_task >> mutations_overlap_tasks
     sifting_task >> mutations_overlap_tasks
     individuals_merge_task >> frequency_tasks
     sifting_task >> frequency_tasks
