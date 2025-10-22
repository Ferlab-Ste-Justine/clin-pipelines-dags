from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import DagRun
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule
from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks import batch_type
from lib.tasks.params_validate import validate_color
from lib.utils_etl import color, spark_jar

with DAG(
        dag_id='etl_migrate',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        params={
            'color': Param('', type=['null', 'string']),
            'snv': Param('no', enum=['yes', 'no']),
            'snv_somatic': Param('no', enum=['yes', 'no']),
            'cnv': Param('no', enum=['yes', 'no']),
            'cnv_somatic_tumor_only': Param('no', enum=['yes', 'no']),
            'variants': Param('no', enum=['yes', 'no']),
            'consequences': Param('no', enum=['yes', 'no']),
            'exomiser': Param('no', enum=['yes', 'no']),
            'exomiser_cnv': Param('no', enum=['yes', 'no']),
            'coverage_by_gene': Param('no', enum=['yes', 'no']),
            'franklin': Param('no', enum=['yes', 'no']),
            'nextflow': Param('no', enum=['yes', 'no']),
            'spark_jar': Param('', type=['null', 'string']),
        },
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_tasks=4,
        max_active_runs=1,
) as dag:

    params_validate_task = validate_color(
        color=color()
    )

    ingest_fhir_task = ingest_fhir(
        batch_ids=[],
        color=color(),
        skip_all='',
        skip_import='yes',  # always skip import, not the purpose of that dag
        skip_batch='',  # we want to do fhir normalized once
        spark_jar=spark_jar(),
    )

    @task(task_id='get_migrate_batch_dag_config')
    def get_migrate_batch_dag_config(batch_id: str) -> dict:
        context = get_current_context()
        params = context["params"]
        return {
            'batch_id': batch_id,
            'export_fhir': 'no', # export already done here, required if etl_migrate_batch is launch manually
            'color': params['color'],
            'snv': params['snv'],
            'snv_somatic': params['snv_somatic'],
            'cnv': params['cnv'],
            'cnv_somatic_tumor_only': params['cnv_somatic_tumor_only'],
            'variants': params['variants'],
            'consequences': params['consequences'],
            'exomiser': params['exomiser'],
            'exomiser_cnv': params['exomiser_cnv'],
            'coverage_by_gene': params['coverage_by_gene'],
            'franklin': params['franklin'],
            'nextflow': params['nextflow'],
            'spark_jar': params['spark_jar'],
        }

    get_all_batch_ids_task = batch_type.get_all_batch_ids()
    get_migrate_batch_dag_config_task = get_migrate_batch_dag_config.expand(batch_id=get_all_batch_ids_task)

    trigger_migrate_batch_dags = TriggerDagRunOperator.partial(
        task_id='etl_migrate_batch',
        trigger_dag_id='etl_migrate_batch',
        wait_for_completion=True
    ).expand(conf=get_migrate_batch_dag_config_task)

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    params_validate_task >> ingest_fhir_task >> get_all_batch_ids_task >> trigger_migrate_batch_dags >> slack
