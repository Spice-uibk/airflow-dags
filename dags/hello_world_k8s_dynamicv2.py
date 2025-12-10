from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import json

default_args = {
    'owner': 'stefanpedratscher',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'chained_branching_k8s',
    default_args=default_args,
    description='Flow: 4 Pods -> Branch -> 2 Pods (Stage 2) -> Summary',
    schedule='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['k8s', 'branching'],
)

with dag:
    start_task = EmptyOperator(task_id='start')

    # ---------------------------------------------------------
    # STEP 1: Generate Initial Workload (4 Items)
    # ---------------------------------------------------------
    @task
    def get_initial_input():
        return ["Data_A", "Data_B", "Data_C", "Data_D"]

    list_of_4 = get_initial_input()

    def generate_cmd(data_input):
        # Python script to run inside the pod
        script = (
            f'import json; '
            f'result = "Processed {data_input}"; '
            f'print(result); '
            f'open("/airflow/xcom/return.json", "w").write(json.dumps(result))'
        )
        return [script]

    # ---------------------------------------------------------
    # STEP 2: Stage 1 (4 Parallel Pods)
    # ---------------------------------------------------------
    stage_1_pods = KubernetesPodOperator.partial(
        task_id='stage_1_processing',
        name='pod-stage-1',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        in_cluster=True,
        do_xcom_push=True,
    ).expand(
        arguments=list_of_4.map(generate_cmd)
    )

    # ---------------------------------------------------------
    # STEP 3: Branching Logic
    # ---------------------------------------------------------
    # This task analyzes the 4 results from Stage 1.
    # It decides whether to proceed to Stage 2 or skip to end.
    @task.branch
    def check_results_and_branch(results):
        print(f"Analyzing {len(results)} results from Stage 1...")

        # LOGIC: If we have 4 results, proceed. Otherwise skip.
        if len(results) == 4:
            return "prepare_stage_2" # Task ID of the next step
        else:
            return "skip_stage_2"    # Task ID of the skip step

    branch_decision = check_results_and_branch(stage_1_pods.output)

    # ---------------------------------------------------------
    # PATH A: Prepare Stage 2 (The Success Path)
    # ---------------------------------------------------------
    @task(task_id="prepare_stage_2")
    def transform_data():
        # Requirement: Parallelism degree of 2 for Stage 2
        print("Branch chosen: Proceeding to Stage 2 with 2 parallel tasks.")
        return ["Stage2_Task_1", "Stage2_Task_2"]

    list_of_2 = transform_data()

    # Stage 2 (2 Parallel Pods)
    stage_2_pods = KubernetesPodOperator.partial(
        task_id='stage_2_processing',
        name='pod-stage-2',
        namespace='stefan-dev',
        image='python:3.9-slim',
        cmds=['python', '-c'],
        in_cluster=True,
        do_xcom_push=True,
    ).expand(
        arguments=list_of_2.map(generate_cmd)
    )

    # ---------------------------------------------------------
    # PATH B: Skip Path
    # ---------------------------------------------------------
    skip_dummy = EmptyOperator(task_id="skip_stage_2")

    # ---------------------------------------------------------
    # STEP 4: Final Summary (Join)
    # ---------------------------------------------------------
    # CRITICAL: trigger_rule='none_failed_min_one_success' or 'all_done'
    # works best for joining branches. Since we are using XComs, we handle
    # the case where stage_2_pods might be skipped (None).
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def final_output(final_results=None):
        print("-----------------------------------------")
        if final_results:
            print(f"Workflow Finished via Stage 2. Results: {final_results}")
        else:
            print("Workflow Finished via Skip path. No Stage 2 results.")
        print("-----------------------------------------")

    # Connect the join task.
    # Note: If branch goes to 'skip', stage_2_pods will be skipped,
    # and final_output will receive None or be skipped depending on trigger rule.
    summary = final_output(stage_2_pods.output)

    end_task = EmptyOperator(task_id='end')

    # ---------------------------------------------------------
    # Dependency Chain
    # ---------------------------------------------------------

    # 1. Start -> List -> Stage 1 -> Branch
    start_task >> list_of_4 >> stage_1_pods >> branch_decision

    # 2. Define the Branch Options
    branch_decision >> list_of_2 >> stage_2_pods # Path A
    branch_decision >> skip_dummy                # Path B

    # 3. Join back to Summary
    stage_2_pods >> summary
    skip_dummy >> summary

    summary >> end_task
