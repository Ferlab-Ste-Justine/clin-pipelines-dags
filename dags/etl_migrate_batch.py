from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.groups.migrate.migrate_germline import migrate_germline
from lib.groups.migrate.migrate_somatic_tumor_normal import \
    migrate_somatic_tumor_normal
from lib.groups.migrate.migrate_somatic_tumor_only import \
    migrate_somatic_tumor_only
from lib.slack import Slack
from lib.tasks import batch_type
from lib.tasks.params_validate import validate_color
from lib.utils_etl import batch_id, color, spark_jar

with DAG(
        dag_id='etl_migrate_batch',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        params={
            'batch_id': Param('', type=['null', 'string']),
            'export_fhir': Param('yes', enum=['yes', 'no']),
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
    def format_skip_condition(param: str) -> str:
        return '{% if params.' + param + ' == "yes" %}{% else %}yes{% endif %}'

    def skip_export_fhir() -> str:
        return format_skip_condition('export_fhir')

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


    def skip_exomiser_cnv() -> str:
        return format_skip_condition('exomiser_cnv')


    def skip_coverage_by_gene() -> str:
        return format_skip_condition('coverage_by_gene')


    def skip_franklin() -> str:
        return format_skip_condition('franklin')


    def skip_nextflow() -> str:
        return format_skip_condition('nextflow')


    params_validate_task = validate_color(
        color=color()
    )

    ingest_fhir_task = ingest_fhir(
        batch_ids=[],
        color=color(),
        skip_all=skip_export_fhir(),
        skip_import='yes',  # always skip import, not the purpose of that dag
        skip_post_import='',  # always regenerate enrich clinical table
        spark_jar=spark_jar(),
    )

    detect_batch_type_task = batch_type.detect(batch_id=batch_id())

    migrate_germline_group = migrate_germline(
        batch_id=batch_id(),
        skip_snv=skip_snv(),
        skip_cnv=skip_cnv(),
        skip_variants=skip_variants(),
        skip_consequences=skip_consequences(),
        skip_exomiser=skip_exomiser(),
        skip_exomiser_cnv=skip_exomiser_cnv(),
        skip_coverage_by_gene=skip_coverage_by_gene(),
        skip_franklin=skip_franklin(),
        skip_nextflow=skip_nextflow(),
        spark_jar=spark_jar()
    )

    migrate_somatic_tumor_only_group = migrate_somatic_tumor_only(
        batch_id=batch_id(),
        skip_snv_somatic=skip_snv_somatic(),
        skip_cnv_somatic_tumor_only=skip_cnv_somatic_tumor_only(),
        skip_variants=skip_variants(),
        skip_consequences=skip_consequences(),
        skip_coverage_by_gene=skip_coverage_by_gene(),
        spark_jar=spark_jar()
    )

    migrate_somatic_tumor_normal_group = migrate_somatic_tumor_normal(
        batch_id=batch_id(),
        skip_snv_somatic=skip_snv_somatic(),
        skip_variants=skip_variants(),
        skip_consequences=skip_consequences(),
        skip_coverage_by_gene=skip_coverage_by_gene(),
        spark_jar=spark_jar()
    )

    detect_batch_type_task >> [migrate_germline_group,
                               migrate_somatic_tumor_only_group,
                               migrate_somatic_tumor_normal_group]

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    params_validate_task >> ingest_fhir_task >> detect_batch_type_task >> migrate_germline_group >> migrate_somatic_tumor_only_group >> migrate_somatic_tumor_normal_group >> slack
