from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from lib import config
from lib.config import K8sContext
from lib.doc import phenovar as doc
from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.groups.phenovar.phenovar_create import phenovar_create
from lib.groups.phenovar.phenovar_update import phenovar_update
from lib.operators.pipeline import PipelineOperator
from lib.phenovar import delete_phenovar_s3_data
from lib.slack import Slack
from lib.tasks import batch_type, clinical, params, params_validate
from lib.utils_etl import ClinAnalysis, color, skip_import, spark_jar, skip_export_fhir

with DAG(
        dag_id='etl_import_phenovar',
        doc_md=doc.phenovar,
        start_date=datetime(2022, 1, 1),
        schedule=None,
        render_template_as_native_obj=True,
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        params={
            'batch_ids': Param([], type=['null', 'array'], description='Put a single batch id per line.'),
            'analysis_ids': Param([], type=['null', 'array'], description='Put a single analysis id per line.'),
            'color': Param('', type=['null', 'string']),
            'export_fhir': Param('yes', enum=['yes', 'no']),
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
        skip_all=skip_export_fhir(),
        skip_import=skip_import(batch_param_name='batch_ids'),
        skip_post_import='',  # Always run enrich clinical steps
        spark_jar=spark_jar()
    )

    identifier_to_type = batch_type.detect(batch_ids=batch_ids, analysis_ids=analysis_ids)

    @task
    def phenovar_validate(_identifier_to_type: dict[str, str]):
        """Validate that only GERMLINE analyses are allowed for Phenovar."""
        if not all(analysis_type == ClinAnalysis.GERMLINE.value for analysis_type in _identifier_to_type.values()):
            raise AirflowFailException("Only GERMLINE analyses are allowed for Phenovar.")

    get_all_analysis_ids = clinical.get_all_analysis_ids(analysis_ids=analysis_ids, batch_ids=batch_ids)

    @task
    def reset_phenovar_data(_analysis_ids: List[str], _reset: str):
        """Delete existing Phenovar S3 data if reset is requested."""
        if _reset != 'yes':
            raise AirflowSkipException('Reset not requested')
        
        s3 = S3Hook(config.s3_conn_id)
        delete_phenovar_s3_data(s3, _analysis_ids)

    reset_task = reset_phenovar_data(get_all_analysis_ids, reset())

    create = phenovar_create(
        analysis_ids=get_all_analysis_ids,
        skip=''
    )

    update = phenovar_update(
        analysis_ids=get_all_analysis_ids,
        skip=''
    )

    @task(task_id='prepare_phenovar_documents_analysis_ids')
    def prepare_phenovar_documents_analysis_ids(analysis_ids: List[str], skip: str) -> str:
        """Prepare analysis IDs argument for AddPhenovarDocuments Java class."""
        if skip:
            raise AirflowSkipException("Skipping Phenovar documents preparation task.")
        if not analysis_ids:
            raise AirflowSkipException("No analyses were submitted to Phenovar.")
        return '--analysis-ids=' + ','.join(analysis_ids)

    prepare_analysis_ids_task = prepare_phenovar_documents_analysis_ids(get_all_analysis_ids, skip='')

    add_phenovar_documents_task = PipelineOperator(
        task_id='add_phenovar_documents',
        name='add_phenovar_documents',
        k8s_context=K8sContext.DEFAULT,
        color=color,
        max_active_tis_per_dag=10,
        arguments=[
            'bio.ferlab.clin.etl.AddPhenovarDocuments',
            prepare_analysis_ids_task,
            '--phenovar-output-folder=' + f'{config.clin_datalake_bucket}/raw/landing/phenovar',
        ],
        skip=''
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    # DAG structure
    ([batch_ids, analysis_ids] >> params_validate_task >> ingest_fhir_group >> identifier_to_type >>
     phenovar_validate(identifier_to_type) >> get_all_analysis_ids >> reset_task >>
     create >> update >> prepare_analysis_ids_task >> add_phenovar_documents_task >> slack)
