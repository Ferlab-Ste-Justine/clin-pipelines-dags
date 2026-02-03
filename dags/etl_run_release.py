from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from lib.config import Env, env, clin_datalake_bucket, etl_run_pending_folder, s3_conn_id
from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.groups.nextflow.nextflow_germline import nextflow_germline
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks import batch_type
from lib.tasks.clinical import get_all_analysis_ids
from lib.tasks.params import get_sequencing_ids
from lib.tasks.params_validate import validate_color
from lib.utils_etl import (ClinAnalysis, color, get_germline_analysis_ids,
                           get_ingest_dag_configs_by_analysis_ids, spark_jar)

with DAG(
        dag_id='etl_run_release',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        catchup=False,
        params={
            'color': Param('', type=['null', 'string']),
            'spark_jar': Param('', type=['null', 'string']),
        },
        render_template_as_native_obj=True,
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_tasks=1,
        max_active_runs=1
) as dag:

    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )

    # Disabling callback as the start task already perform the slack notification
    params_validate_color = validate_color.override(on_execute_callback=None)(color=color())

    ingest_fhir_group = ingest_fhir(
        batch_ids=[],  # No associated "batch"
        color=params_validate_color,
        skip_all=False,
        skip_import=True,  # Skipping because the data is already imported via the prescription API
        skip_post_import=False,
        spark_jar=spark_jar()
    )

    @task(task_id='get_pending_sequencing_ids')
    def get_pending_sequencing_ids() -> list:
        """
        Read sequencing IDs from all .txt filenames in the .etl-run folder on S3.
        """
        s3 = S3Hook(s3_conn_id)
        
        # List all files in the .etl-run folder
        keys = s3.list_keys(bucket_name=clin_datalake_bucket, prefix=f'{etl_run_pending_folder}/')
        
        if not keys:
            return []
        
        # Filter only .txt files and extract sequencing IDs from filenames
        sequencing_ids = []
        for key in keys:
            if key.endswith('.txt'):
                # Extract sequencing ID from filename: remove prefix and .txt extension
                filename = key.replace(f'{etl_run_pending_folder}/', '').replace('.txt', '')
                if filename:
                    sequencing_ids.append(filename)
        
        return sequencing_ids
    
    get_sequencing_ids_task = get_pending_sequencing_ids()
    get_all_analysis_ids_task = get_all_analysis_ids(sequencing_ids=get_sequencing_ids_task)

    detect_batch_types_task = batch_type.detect(analysis_ids=get_all_analysis_ids_task, allowMultipleIdentifierTypes=True)

    get_germline_analysis_ids_task = get_germline_analysis_ids(all_batch_types=detect_batch_types_task, analysis_ids=get_all_analysis_ids_task)
    nextflow_germline_task_group = nextflow_germline(analysis_ids=get_germline_analysis_ids_task)

    @task(task_id='check_should_skip_franklin')
    def check_should_skip_franklin(germline_analysis_ids: list[str]) -> str:
        return 'yes' if len(germline_analysis_ids) == 0 or env == Env.QA else ''

    check_should_skip_franklin_task = check_should_skip_franklin(get_germline_analysis_ids_task)

    trigger_franklin_by_analysis_id_dags = TriggerDagRunOperator(
        task_id='import_franklin',
        trigger_dag_id='etl_import_franklin',
        wait_for_completion=True,
        skip=check_should_skip_franklin_task,
        conf={
            'batch_ids': None,
            'analysis_ids': get_germline_analysis_ids_task,
            'color': params_validate_color,
            'import': 'no',
            'spark_jar': spark_jar(),
        }
    )

    @task(task_id='cleanup_pending_sequencing_ids')
    def cleanup_pending_sequencing_ids(sequencing_ids: list) -> str:
        """
        Delete only the .txt files from the .etl-run folder that were processed in this run.
        """
        if not sequencing_ids or len(sequencing_ids) == 0:
            return "No files to delete"
        
        s3 = S3Hook(s3_conn_id)
        
        for seq_id in sequencing_ids:
            # Reconstruct the filename for each processed sequencing ID
            s3_key = f'{etl_run_pending_folder}/{seq_id}.txt'
            s3.delete_objects(bucket=clin_datalake_bucket, keys=s3_key)
           
        return f"Deleted {sequencing_ids} processed files from S3"
    
    cleanup_task = cleanup_pending_sequencing_ids(get_sequencing_ids_task)

    trigger_etl = TriggerDagRunOperator(
        task_id='trigger_etl',
        trigger_dag_id='etl',
        wait_for_completion=True,
        conf={
            'analysis_ids': get_all_analysis_ids_task,
            'notify_slack': 'yes' if env == Env.PROD else 'no',
            'color': params_validate_color,
            'spark_jar': spark_jar(),
        }
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    (
        start >> params_validate_color >>
        ingest_fhir_group >>
        get_sequencing_ids_task >> get_all_analysis_ids_task >>
        detect_batch_types_task >>
        get_germline_analysis_ids_task >> nextflow_germline_task_group >>
        check_should_skip_franklin_task >> trigger_franklin_by_analysis_id_dags >>
        cleanup_task >> trigger_etl >>
        slack
    )
