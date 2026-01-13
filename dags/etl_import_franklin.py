from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.doc import franklin as doc
from lib.franklin import delete_franklin_s3_data
from lib.groups.franklin.franklin_create import franklin_create
from lib.groups.franklin.franklin_update import franklin_update
from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.slack import Slack
from lib.tasks import batch_type, clinical, params, params_validate
from lib.utils_etl import (ClinAnalysis, color, skip_import,
                           spark_jar)

with DAG(
        dag_id='etl_import_franklin',
        doc_md=doc.franklin,
        start_date=datetime(2022, 1, 1),
        schedule=None,
        render_template_as_native_obj=True,
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        params={
            'batch_ids': Param([], type=['null', 'array'], description='Put a single batch id per line.'),
            'analysis_ids': Param([], type=['null', 'array'], description='Put a single sequencing id per line.'),
            'color': Param('', type=['null', 'string']),
            'import': Param('yes', enum=['yes', 'no']),
            'reset': Param('no', enum=['yes', 'no']),
            'spark_jar': Param('', type=['null', 'string']),
        },
        max_active_tasks=1,
        max_active_runs=1,
) as dag:

    batch_ids: List[str] = params.get_batch_ids()
    analysis_ids: List[str] = params.get_analysis_ids()
    color: str = color()

    def reset() -> str:
        return '{{ params.reset or "" }}'

    params_validate_task = params_validate.validate_batch_ids_analysis_ids_color(batch_ids, analysis_ids, color)

    ingest_fhir_group = ingest_fhir(
        batch_ids=batch_ids,
        color=color,
        skip_all='',  # Always run
        skip_import=skip_import(batch_param_name='batch_ids'),
        skip_post_import='',  # Always run enrich clinical steps
        spark_jar=spark_jar()
    )

    identifier_to_type = batch_type.detect(batch_ids=batch_ids, analysis_ids=analysis_ids)

    @task
    def franklin_validate(_identifier_to_type: dict[str, str]):
        # Only GERMLINE analyses are allowed
        if not all(analysis_type == ClinAnalysis.GERMLINE.value for analysis_type in _identifier_to_type.values()):
            raise AirflowFailException("Only GERMLINE analyses are allowed for Franklin.")

    get_all_analysis_ids = clinical.get_all_analysis_ids(analysis_ids=analysis_ids, batch_ids=batch_ids)

    @task
    def reset_franklin_data(_analysis_ids: List[str], _reset: str):
        if _reset != 'yes':
            raise AirflowSkipException('Reset not requested')
        
        s3 = S3Hook(config.s3_conn_id)
        delete_franklin_s3_data(s3, _analysis_ids)

    reset_task = reset_franklin_data(get_all_analysis_ids, reset())

    create = franklin_create(
        analysis_ids=get_all_analysis_ids,
        skip=''
    )

    update = franklin_update(
        analysis_ids=get_all_analysis_ids,
        skip=''
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    ([batch_ids, analysis_ids] >> params_validate_task >> ingest_fhir_group >> identifier_to_type >>
     franklin_validate(identifier_to_type) >> get_all_analysis_ids >> reset_task >> create >> update >> slack)
