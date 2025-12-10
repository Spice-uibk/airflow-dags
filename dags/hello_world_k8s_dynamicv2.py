from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
import json

default_args = {
    'owner': 'stefanpedratscher',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'chained_parallelism_k8s',
    default_args=default_args,
    description='Flow: 1 -> 4 Pods -> Transform -> 3 Pods -> Summary',
    schedule='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['k8s', 'parallelism'],
)

with dag:
    start_task = EmptyOperator(task_id='start')

    # ---------------------------------------------------------
    # STEP 1: Generate Initial Workload (4 Items)
    # ---------------------------------------------------------
    @task
    def get_initial_input():
        print("Generating list for 4 parallel tasks...")
        return ["Data_A", "Data_B", "Data_C", "Data_D"]

    list_of_4 = get_initial_input()

    # Helper to generate Python script for Pods
    # It prints to logs AND writes to XCom JSON file
    def generate_cmd(data_input):
        script = (
            f'import json; '
            f'result = "Processed {data_input}"; '
            f'print(result); '
            f'open("/airflow/xcom/return.json", "w").write(json.dumps(result))'
        )
        return [script]

    # ---------------------------------------------------------
    # STEP 2: First Fan-Out (4 Parallel Pods)
    # ---------------------------------------------------------
    stage_1_pods = KubernetesPodOperator.partial(
        task_id='stage_1_processing',
        name='pod-stage-1',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        in_cluster=True,
        do_xcom_push=True, # Critical for collecting results
    ).expand(
        arguments=list_of_4.map(generate_cmd)
    )

    # ---------------------------------------------------------
    # STEP 3: Transform (Reduce 4 -> Map 3)
    # ---------------------------------------------------------
    # This task acts as a barrier. It waits for the 4 pods to finish,
    # receives their output, and generates a NEW list of 3 items.
    @task
    def transform_4_to_3(previous_results):
        print(f"Stage 1 finished. Received {len(previous_results)} outputs: {previous_results}")

        # Example Logic: Create 3 new batches based on the previous 4 results
        # In a real scenario, you might aggregate data here.
        new_workload = [
            f"Batch_1 (from {len(previous_results)} inputs)",
            f"Batch_2 (Analysis)",
            f"Batch_3 (Cleanup)"
        ]
        return new_workload

    list_of_3 = transform_4_to_3(stage_1_pods.output)

    # ---------------------------------------------------------
    # STEP 4: Second Fan-Out (3 Parallel Pods)
    # ---------------------------------------------------------
    stage_2_pods = KubernetesPodOperator.partial(
        task_id='stage_2_processing',
        name='pod-stage-2',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        in_cluster=True,
        do_xcom_push=True,
    ).expand(
        arguments=list_of_3.map(generate_cmd)
    )

    # ---------------------------------------------------------
    # STEP 5: Final Summary (Fan-In)
    # ---------------------------------------------------------
    @task
    def final_output(final_results):
        print("-----------------------------------------")
        print("WORKFLOW COMPLETE")
        print(f"Final Stage produced {len(final_results)} items:")
        for res in final_results:
            print(f" - {res}")
        print("-----------------------------------------")

    end_summary = final_output(stage_2_pods.output)

    end_task = EmptyOperator(task_id='end')

    # ---------------------------------------------------------
    # Dependencies
    # ---------------------------------------------------------
    # The dependencies are implicit in the data flow (.output),
    # but we chain the start/end markers explicitly.
    start_task >> list_of_4
    # The middle is handled by data passing (XComs)
    end_summary >> end_task
