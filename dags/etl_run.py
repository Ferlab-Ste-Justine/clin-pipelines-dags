from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from lib.config import clin_datalake_bucket, etl_run_pending_folder, s3_conn_id
from lib.slack import Slack
from lib.tasks.params import get_sequencing_ids

with DAG(
        dag_id='etl_run',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        catchup=False,
        params={
            'sequencing_ids': Param([], type=['null', 'array']),
            'spark_jar': Param('', type=['null', 'string']),
        },
        render_template_as_native_obj=True,
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_runs=10 # Allow multiple concurrent runs, we are just writing files to S3
) as dag:

    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )

    @task(task_id='save_sequencing_ids_to_s3')
    def save_sequencing_ids_to_s3(sequencing_ids: list) -> str:
        """
        Save sequencing IDs to S3 in the .etl-run folder for later processing by etl_run_pending.
        Each sequencing ID is saved as a separate file named with the sequencing ID.
        """
        if not sequencing_ids or len(sequencing_ids) == 0:
            return "No sequencing IDs to save"
        
        s3 = S3Hook(s3_conn_id)

        for seq_id in sequencing_ids:
            # Create a file for each sequencing ID in the .etl-run folder
            s3_key = f'{etl_run_pending_folder}/{seq_id}.txt'
            # in the future we might want to save more info in the file, for now just create an empty file
            s3.load_string("", s3_key, clin_datalake_bucket, replace=True)
        
        return f"Saved {len(sequencing_ids)} sequencing IDs to S3 in .etl-run/"
    
    save_task = save_sequencing_ids_to_s3(get_sequencing_ids())

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> save_task >> slack
