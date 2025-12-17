from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
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
CHROM_NR = "22"
MINIO_BUCKET = "genome-data"
KEY_INPUT_INDIVIDUAL = "ALL.chr22.80000.vcf.gz"
KEY_INPUT_SIFTING = "ALL.chr22.phase3_shapeit2_mvncall_integrated_v5.20130502.sites.annotation.vcf.gz"

NAMESPACE = "stefan-dev"

# Constants for frequency task
FREQ_TOTAL_PLOTS = 1000
FREQ_PARALLELISM = 5
FREQ_CHUNK_SIZE = FREQ_TOTAL_PLOTS // FREQ_PARALLELISM

# Environment variables for all pods
minio_env_vars = [
    k8s.V1EnvVar(name="MINIO_ENDPOINT", value=MINIO_ENDPOINT),
    k8s.V1EnvVar(name="MINIO_ACCESS_KEY", value=MINIO_ACCESS_KEY),
    k8s.V1EnvVar(name="MINIO_SECRET_KEY", value=MINIO_SECRET_KEY),
    k8s.V1EnvVar(name="MINIO_SECURE", value="false"),  # set to true for HTTPS
]

with DAG(
        dag_id='genome_data_processing_par',
        default_args=default_args,
        description='Genome processing pipeline using KubernetesPodOperator',
        schedule=None,
        catchup=False,
        tags=['genome', 'kubernetes', 'minio'],
        max_active_tasks=42,  # TODO: maybe check??
) as dag:

    LOOP_LIMIT = int(Variable.get("genome_individuals_parallelism_count", default_var=5))
    CHUNK_SIZE = int(Variable.get("genome_individuals_chunk_size", default_var=2000))

    # Individual task
    individual_tasks = []
    for x in range(LOOP_LIMIT):  # maybe change constant and change step size accordingly
        counter = x * CHUNK_SIZE + 1
        stop = (x + 1) * CHUNK_SIZE + 1

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
            env_vars=minio_env_vars,
            get_logs=True,
            is_delete_operator_pod=True,
            image_pull_policy="IfNotPresent",
            execution_timeout=timedelta(hours=1),
            node_selector={"kubernetes.io/hostname": "node1"},
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
        env_vars=minio_env_vars,
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        execution_timeout=timedelta(hours=1),
        node_selector={"kubernetes.io/hostname": "node1"},
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
            "--keys", ','.join([f'chr22n-{x * CHUNK_SIZE + 1}-{(x + 1) * CHUNK_SIZE + 1}.tar.gz' for x in range(LOOP_LIMIT)]),  # take same step size and interation limit as in first loop
            "--bucket_name", MINIO_BUCKET
        ],
        env_vars=minio_env_vars,
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        execution_timeout=timedelta(hours=1),
        node_selector={"kubernetes.io/hostname": "node1"},
    )

    # Mutations Overlap task
    # mutations_overlap_tasks = []
    pop_arr = ["EUR", "AFR", "EAS", "ALL", "GBR", "SAS", "AMR"]

    # for pop in pop_arr:
    #     task = KubernetesPodOperator(
    #         task_id=f"mutations_overlap_{pop}",
    #         name=f"mutations-overlap-{pop.lower()}",
    #         namespace=NAMESPACE,
    #         image="kogsi/genome_dag:mutations-overlap",
    #         cmds=["python3", "mutations-overlap.py"],
    #         arguments=[
    #             "--chromNr", CHROM_NR,
    #             "--POP", pop,
    #             "--bucket_name", MINIO_BUCKET
    #         ],
    #         env_vars=minio_env_vars,
    #         get_logs=True,
    #         is_delete_operator_pod=True,
    #         image_pull_policy="IfNotPresent",
    #         execution_timeout=timedelta(hours=1),
    #         node_selector={"kubernetes.io/hostname": "node1"},
    #     )
    #     mutations_overlap_tasks.append(task)

    # Frequency tasks
    for pop in pop_arr:
        if pop in ["ALL"]:
            # Calculate overlaps and uploads results to MinIO
            freq_calc = KubernetesPodOperator(
                task_id=f"frequency_calc_{pop}",
                name=f"frequency-calc-{pop}",
                namespace=NAMESPACE,
                image="kogsi/genome_dag:frequency_par",
                cmds=["python3", "frequency_par.py"],
                arguments=[
                    "--mode", "calc",
                    "--chromNr", CHROM_NR,
                    "--POP", pop,
                    "--bucket_name", MINIO_BUCKET
                ],
                env_vars=minio_env_vars,
                get_logs=True,
                is_delete_operator_pod=True,
                image_pull_policy="IfNotPresent",
                execution_timeout=timedelta(hours=1),
                node_selector={"kubernetes.io/hostname": "node1"},
            )

            # Merges partial results, uploads one tar.gz
            freq_merge = KubernetesPodOperator(
                task_id=f"frequency_merge_{pop}",
                name=f"frequency-merge-{pop}",
                namespace=NAMESPACE,
                image="kogsi/genome_dag:frequency_par",
                cmds=["python3", "frequency_par.py"],
                arguments=[
                    "--mode", "merge",
                    "--chromNr", CHROM_NR,
                    "--POP", pop,
                    "--bucket_name", MINIO_BUCKET,
                    "--chunks", str(FREQ_PARALLELISM)
                ],
                env_vars=minio_env_vars,
                get_logs=True,
                is_delete_operator_pod=True,
                image_pull_policy="IfNotPresent",
                execution_timeout=timedelta(hours=1),
                node_selector={"kubernetes.io/hostname": "node1"},
            )

            individuals_merge_task >> freq_calc
            sifting_task >> freq_calc

            # Parallel Plotting
            for i in range(FREQ_PARALLELISM):
                start_idx = i * FREQ_CHUNK_SIZE
                end_idx = (i + 1) * FREQ_CHUNK_SIZE if i < FREQ_PARALLELISM else FREQ_TOTAL_PLOTS

                freq_plot = KubernetesPodOperator(
                    task_id=f"frequency_plot_{pop}_{i}",
                    name=f"frequency-plot-{pop}-{i}",
                    namespace=NAMESPACE,
                    image="kogsi/genome_dag:frequency_par",
                    cmds=["python3", "frequency_par.py"],
                    arguments=[
                        "--mode", "plot",
                        "--chromNr", CHROM_NR,
                        "--POP", pop,
                        "--bucket_name", MINIO_BUCKET,
                        "--start", str(start_idx),
                        "--end", str(end_idx),
                        "--chunk_id", str(i),
                    ],
                    env_vars=minio_env_vars,
                    get_logs=True,
                    is_delete_operator_pod=True,
                    image_pull_policy="IfNotPresent",
                    execution_timeout=timedelta(hours=1),
                    node_selector={"kubernetes.io/hostname": "node1"},
                )

                freq_calc >> freq_plot >> freq_merge
        else:
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
                env_vars=minio_env_vars,
                get_logs=True,
                is_delete_operator_pod=False,
                image_pull_policy="IfNotPresent",
                execution_timeout=timedelta(hours=1),
                node_selector={"kubernetes.io/hostname": "node1"},
            )
            individuals_merge_task >> task
            sifting_task >> task
    # Task dependencies
    individual_tasks >> individuals_merge_task
    # individuals_merge_task >> mutations_overlap_tasks
    # sifting_task >> mutations_overlap_tasks