from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from lib.config import clin_datalake_bucket, etl_run_pending_folder, s3_conn_id
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack

with DAG(
        dag_id='etl_run_release_trigger',
        start_date=datetime(2022, 1, 1),
        schedule='0 20 * * *',  # Run every day at 8pm
        catchup=False,
        render_template_as_native_obj=True,
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_runs=1  # Prevents multiple runs from stacking up
) as dag:
    
    @task(task_id='check_pending_sequencing_ids')
    def check_pending_sequencing_ids() -> str:
        """
        Check if there are any pending .txt files in the .etl-run folder on S3.
        Returns 'yes' if files exist, raises AirflowSkipException otherwise.
        """
        s3 = S3Hook(s3_conn_id)
        
        # List all files in the .etl-run folder
        keys = s3.list_keys(bucket_name=clin_datalake_bucket, prefix=f'{etl_run_pending_folder}/')
        
        if not keys:
            raise AirflowSkipException("No pending files found in .etl-run folder")
        
        # Filter only .txt files
        txt_files = [key for key in keys if key.endswith('.txt')]
        
        if not txt_files:
            raise AirflowSkipException("No pending .txt files found in .etl-run folder")
        
        return f"Found {len(txt_files)} pending file(s) to process"

    check_pendings = check_pending_sequencing_ids()

    trigger_etl_run_release = TriggerDagRunOperator(
        task_id='trigger_etl_run_release',
        trigger_dag_id='etl_run_release',
        wait_for_completion=True, # One release at a time
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Only run if check task succeeded (not skipped)
        conf={}
    )

    # no Slack notifications here, just quietly trigger the etl_run_release dag if there are pendings
    check_pendings >> trigger_etl_run_release
