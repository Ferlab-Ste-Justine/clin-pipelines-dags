from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.config import Env, env
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
        dag_id='etl_run',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        catchup=False,
        params={
            'sequencing_ids': Param([], type=['null', 'array']),
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

    get_sequencing_ids_task = get_sequencing_ids()
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

    get_ingest_dag_configs_by_analysis_ids_task = get_ingest_dag_configs_by_analysis_ids.partial(all_batch_types=detect_batch_types_task, analysis_ids=get_all_analysis_ids_task, skip_batch="yes").expand(analysisType=[ClinAnalysis.GERMLINE.value, ClinAnalysis.SOMATIC_TUMOR_ONLY.value])

    trigger_ingest_by_sequencing_ids_dags = TriggerDagRunOperator.partial(
        task_id='ingest_sequencing_ids',
        trigger_dag_id='etl_ingest',
        wait_for_completion=True,
    ).expand(conf=get_ingest_dag_configs_by_analysis_ids_task)

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
        get_ingest_dag_configs_by_analysis_ids_task >> trigger_ingest_by_sequencing_ids_dags >>
        slack
    )
