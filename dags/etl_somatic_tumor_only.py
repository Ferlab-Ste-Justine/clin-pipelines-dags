from datetime import datetime

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule
from etl_qa import spark_jar
from lib.groups.index.index import index
from lib.groups.index.prepare_index import prepare_index
from lib.groups.index.publish_index import publish_index
from lib.groups.ingest.ingest_somatic_tumor_only import \
    ingest_somatic_tumor_only
from lib.groups.qa import qa
from lib.slack import Slack
from lib.tasks import enrich
from lib.tasks.notify import notify
from lib.tasks.params_validate import validate_release_color
from lib.utils_etl import (batch_id, color, default_or_initial, release_id,
                           skip_batch, skip_import, skip_notify)

with DAG(
        dag_id='etl_somatic_tumor_only',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'batch_id': Param('', type='string'),
            'release_id': Param('', type='string'),
            'color': Param('', enum=['', 'blue', 'green']),
            'import': Param('yes', enum=['yes', 'no']),
            'notify': Param('no', enum=['yes', 'no']),
            'spark_jar': Param('', type='string'),
        },
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_tasks=4
) as dag:
    params_validate_task = validate_release_color(
        release_id=release_id(),
        color=color()
    )

    ingest_somatic_tumor_only_group = ingest_somatic_tumor_only(
        batch_id=batch_id(),
        batch_type_detected=False,
        color=color(),
        skip_import=skip_import(),
        skip_batch=skip_batch(),
        skip_snv_somatic=skip_batch(),
        skip_cnv_somatic_tumor_only=skip_batch(),
        skip_variants=skip_batch(),
        skip_consequences=skip_batch(),
        skip_coverage_by_gene=skip_batch(),
        spark_jar=spark_jar()
    )


    @task_group(group_id='enrich')
    def enrich_somatic_tumor_only():
        snv_somatic_all = enrich.snv_somatic_all(spark_jar=spark_jar(), steps=default_or_initial())
        variants = enrich.variants(spark_jar=spark_jar(), steps=default_or_initial())
        consequences = enrich.consequences(spark_jar=spark_jar(), steps=default_or_initial())
        cnv = enrich.cnv_all(spark_jar=spark_jar(), steps=default_or_initial())
        coverage_by_gene = enrich.coverage_by_gene(spark_jar=spark_jar(), steps=default_or_initial())

        snv_somatic_all >> variants >> consequences >> cnv >> coverage_by_gene


    prepare_group = prepare_index(spark_jar=spark_jar())

    qa_group = qa(
        spark_jar=spark_jar()
    )

    index_group = index(
        release_id=release_id(),
        color=color('_'),
        spark_jar=spark_jar()
    )

    publish_group = publish_index(
        release_id=release_id(),
        color=color('_'),
        spark_jar=spark_jar()
    )

    notify_task = notify(
        batch_id=batch_id(),
        color=color(),
        skip=skip_notify()
    )

    params_validate_task >> ingest_somatic_tumor_only_group >> enrich_somatic_tumor_only() >> prepare_group >> qa_group >> index_group >> publish_group >> notify_task
