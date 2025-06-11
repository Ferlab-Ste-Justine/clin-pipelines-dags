from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.groups.ingest.ingest_germline import ingest_germline
from lib.groups.ingest.ingest_somatic_tumor_normal import \
    ingest_somatic_tumor_normal
from lib.groups.ingest.ingest_somatic_tumor_only import \
    ingest_somatic_tumor_only
from lib.slack import Slack
from lib.tasks import batch_type
from lib.tasks.params_validate import (get_sequencing_ids,
                                       validate_batch_sequencing_ids_color)
from lib.utils_etl import (batch_id, color, sequencing_ids, skip_import,
                           spark_jar)

with DAG(
        dag_id='etl_ingest',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        params={
            'batch_id': Param('', type=['null', 'string']),
            'sequencing_ids': Param([], type=['null', 'array']),
            'color': Param('', type=['null', 'string']),
            'import': Param('yes', enum=['yes', 'no']),
            'spark_jar': Param('', type=['null', 'string']),
        },
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure
        },
        render_template_as_native_obj=True,
        max_active_tasks=4,
        max_active_runs=1
) as dag:

    params_validate = validate_batch_sequencing_ids_color(
        batch_id=batch_id(),
        sequencing_ids=sequencing_ids(),
        color=color()
    )

    get_sequencing_ids_task = get_sequencing_ids()

    detect_batch_type_task = batch_type.detect(batch_ids=batch_id(), sequencing_ids=get_sequencing_ids_task)

    ingest_germline_group = ingest_germline(
        batch_id=batch_id(),
        batch_type_detected=True,
        color=color(),
        skip_import=skip_import(),  # skipping already imported batch is allowed
        skip_batch='',  # always compute this batch (purpose of this dag)
        skip_snv='',
        skip_cnv='',
        skip_variants='',
        skip_consequences='',
        skip_exomiser='',
        skip_coverage_by_gene='',
        skip_franklin='',
        skip_nextflow='',
        spark_jar=spark_jar()
    )

    ingest_somatic_tumor_only_group = ingest_somatic_tumor_only(
        batch_id=batch_id(),
        batch_type_detected=True,
        color=color(),
        skip_import=skip_import(),  # skipping already imported batch is allowed
        skip_batch='',  # always compute this batch (purpose of this dag)
        skip_snv_somatic='',
        skip_cnv_somatic_tumor_only='',
        skip_variants='',
        skip_consequences='',
        skip_coverage_by_gene='',
        spark_jar=spark_jar()
    )

    ingest_somatic_tumor_normal_group = ingest_somatic_tumor_normal(
        batch_id=batch_id(),
        batch_type_detected=True,
        color=color(),
        skip_import=skip_import(),  # skipping already imported batch is allowed
        skip_batch='',  # always compute this batch (purpose of this dag)
        skip_snv_somatic='',
        skip_variants='',
        skip_consequences='',
        skip_coverage_by_gene='',
        spark_jar=spark_jar()
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    params_validate >> get_sequencing_ids_task >> detect_batch_type_task >> [ingest_germline_group,
                                                  ingest_somatic_tumor_only_group,
                                                  ingest_somatic_tumor_normal_group] >> slack
