from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from lib.config import batch_ids
from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.groups.migrate.migrate_germline import migrate_germline
from lib.groups.migrate.migrate_somatic_tumor_normal import migrate_somatic_tumor_normal
from lib.groups.migrate.migrate_somatic_tumor_only import migrate_somatic_tumor_only
from lib.slack import Slack
from lib.tasks import batch_type
from lib.tasks.params_validate import validate_color
from lib.utils_etl import color, spark_jar

with DAG(
    dag_id='etl_migrate',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'color': Param('', type=['null', 'string']),
        'snv': Param('no', enum=['yes', 'no']),
        'snv_somatic': Param('no', enum=['yes', 'no']),
        'cnv': Param('no', enum=['yes', 'no']),
        'cnv_somatic_tumor_only': Param('no', enum=['yes', 'no']),
        'variants': Param('no', enum=['yes', 'no']),
        'consequences': Param('no', enum=['yes', 'no']),
        'exomiser': Param('no', enum=['yes', 'no']),
        'coverage_by_gene': Param('no', enum=['yes', 'no']),
        'franklin': Param('no', enum=['yes', 'no']),
        'spark_jar': Param('', type=['null', 'string']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
    max_active_tasks=4
) as dag:
    def format_skip_condition(param: str) -> str:
        return '{% if params.' + param + ' == "yes" %}{% else %}yes{% endif %}'


    def skip_snv() -> str:
        return format_skip_condition('snv')


    def skip_snv_somatic() -> str:
        return format_skip_condition('snv_somatic')


    def skip_cnv() -> str:
        return format_skip_condition('cnv')


    def skip_cnv_somatic_tumor_only() -> str:
        return format_skip_condition('cnv_somatic_tumor_only')


    def skip_variants() -> str:
        return format_skip_condition('variants')


    def skip_consequences() -> str:
        return format_skip_condition('consequences')


    def skip_exomiser() -> str:
        return format_skip_condition('exomiser')


    def skip_coverage_by_gene() -> str:
        return format_skip_condition('coverage_by_gene')


    def skip_franklin() -> str:
        return format_skip_condition('franklin')


    def _concat_batch_id(prefix: str, batch_id: str) -> str:
        return prefix + '_' + batch_id.replace('.', '')  # '.' not allowed


    params_validate_task = validate_color.override(on_execute_callback=Slack.notify_dag_start)(
        color=color()
    )

    all_dags = ingest_fhir(
        batch_id='',
        color=color(),
        skip_import='yes',  # always skip import, not the purpose of that dag
        skip_batch='',  # we want to do fhir normalized once
        spark_jar=spark_jar(),
    )

    params_validate_task >> all_dags


    def migrate_batch_id(batch_id: str) -> TaskGroup:
        with TaskGroup(group_id=_concat_batch_id('migrate', batch_id)) as group:
            @task.branch(task_id='call_group')
            def call_migrate_group(batch_type: str):
                batch_type_migrate_map = {
                    'germline': ['validate_germline', 'migrate_germline'],
                    'somatic_tumor_only': ['validate_somatic_tumor_only', 'migrate_somatic_tumor_only'],
                    'somatic_tumor_normal': ['validate_somatic_tumor_normal', 'migrate_somatic_tumor_normal']
                }
                return batch_type_migrate_map[batch_type]

            call_migrate_group_task = call_migrate_group(batch_type.detect(batch_id))

            migrate_germline_group = migrate_germline(
                batch_id=batch_id,
                skip_snv=skip_snv(),
                skip_cnv=skip_cnv(),
                skip_variants=skip_variants(),
                skip_consequences=skip_consequences(),
                skip_exomiser=skip_exomiser(),
                skip_coverage_by_gene=skip_coverage_by_gene(),
                skip_franklin=skip_franklin(),
                spark_jar=spark_jar()
            )

            migrate_somatic_tumor_only_group = migrate_somatic_tumor_only(
                batch_id=batch_id,
                skip_snv_somatic=skip_snv_somatic(),
                skip_cnv_somatic_tumor_only=skip_cnv_somatic_tumor_only(),
                skip_variants=skip_variants(),
                skip_consequences=skip_consequences(),
                skip_coverage_by_gene=skip_coverage_by_gene(),
                spark_jar=spark_jar()
            )

            migrate_somatic_tumor_normal_group = migrate_somatic_tumor_normal(
                batch_id=batch_id,
                skip_snv_somatic=skip_snv_somatic(),
                skip_variants=skip_variants(),
                skip_consequences=skip_consequences(),
                skip_coverage_by_gene=skip_coverage_by_gene(),
                spark_jar=spark_jar()
            )

            call_migrate_group_task >> [migrate_germline_group, migrate_somatic_tumor_only_group,
                                        migrate_somatic_tumor_normal_group]

        return group


    # concat every dags inside a loop
    for batch_id in batch_ids:
        batch = migrate_batch_id(batch_id)
        all_dags >> batch
        all_dags = batch

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    all_dags >> slack
