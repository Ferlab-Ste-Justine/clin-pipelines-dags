from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from lib.doc import franklin as doc
from lib.groups.franklin.franklin_create import franklin_create
from lib.groups.franklin.franklin_update import franklin_update
from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.slack import Slack
from lib.tasks import params, batch_type, params_validate, clinical
from lib.utils_etl import color, skip_import, spark_jar, ClinSchema, ClinAnalysis

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
            'sequencing_ids': Param([], type=['null', 'array'], description='Put a single sequencing id per line.'),
            'color': Param('', type=['null', 'string']),
            'import': Param('yes', enum=['yes', 'no']),
            'spark_jar': Param('', type=['null', 'string']),
        },
) as dag:
    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )

    batch_ids: List[str] = params.get_batch_ids()
    sequencing_ids: List[str] = params.get_sequencing_ids()
    color: str = color()

    params_validate_task = params_validate.validate_batch_ids_sequencing_ids_color(batch_ids, sequencing_ids, color)

    ingest_fhir_group = ingest_fhir(
        batch_ids=batch_ids,
        color=color,
        skip_all='',  # Always run
        skip_import=skip_import(batch_param_name='batch_ids'),
        skip_batch='',  # Always compute these batches
        spark_jar=spark_jar()
    )

    identifier_to_type = batch_type.detect(batch_ids=batch_ids, sequencing_ids=sequencing_ids)


    @task
    def franklin_validate(_identifier_to_type: dict[str, str]):
        # Only GERMLINE analyses are allowed
        if not all(analysis_type == ClinAnalysis.GERMLINE.value for analysis_type in _identifier_to_type.values()):
            raise AirflowFailException("Only GERMLINE analyses are allowed for Franklin.")


    analysis_ids = clinical.get_all_analysis_ids(sequencing_ids, batch_ids)

    create = franklin_create(
        analysis_ids=analysis_ids,
        skip=''
    )

    update = franklin_update(
        analysis_ids=analysis_ids,
        skip=''
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    (start >> [batch_ids, sequencing_ids] >> params_validate_task >> ingest_fhir_group >> identifier_to_type >>
     franklin_validate(identifier_to_type) >> analysis_ids >> create >> update >> slack)
