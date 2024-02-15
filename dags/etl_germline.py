from datetime import datetime

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from etl_qa import release_id, spark_jar
from lib.groups.index.index import index
from lib.groups.ingest.ingest_germline import ingest_germline
from lib.groups.index.prepare_index import prepare_index
from lib.groups.index.publish_index import publish_index
from lib.slack import Slack
from lib.tasks import (enrich, qa)
from lib.tasks.notify import notify
from lib.tasks.params_validate import validate_release_color
from lib.utils_etl import color, batch_id, skip_import, skip_batch, default_or_initial, skip_notify

with DAG(
        dag_id='etl_germline',
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

    ingest_germline_group = ingest_germline(
        batch_id=batch_id(),
        color=color(),
        skip_import=skip_import(),
        skip_batch=skip_batch(),
        skip_snv=skip_batch(),
        skip_cnv=skip_batch(),
        skip_variants=skip_batch(),
        skip_consequences=skip_batch(),
        skip_exomiser=skip_batch(),
        skip_coverage_by_gene=skip_batch(),
        skip_franklin=skip_batch(),
        spark_jar=spark_jar(),
    )


    @task_group(group_id='enrich')
    def enrich_germline():
        snv = enrich.snv(steps=default_or_initial(), spark_jar=spark_jar())
        variants = enrich.variants(spark_jar=spark_jar(), steps=default_or_initial())
        consequences = enrich.consequences(spark_jar=spark_jar(), steps=default_or_initial())
        cnv = enrich.cnv(spark_jar=spark_jar(), steps=default_or_initial())
        coverage_by_gene = enrich.coverage_by_gene(spark_jar=spark_jar(), steps=default_or_initial())

        snv >> variants >> consequences >> cnv >> coverage_by_gene


    prepare_group = prepare_index(
        release_id=release_id(),
        spark_jar=spark_jar()
    )


    @task_group(group_id='qa')
    def qa_germline():
        non_empty_tables = qa.non_empty_tables(release_id(), spark_jar())
        no_dup_gnomad = qa.no_dup_gnomad(release_id(), spark_jar())
        no_dup_nor_snv = qa.no_dup_nor_snv(release_id(), spark_jar())
        no_dup_nor_consequences = qa.no_dup_nor_consequences(release_id(), spark_jar())
        no_dup_nor_variants = qa.no_dup_nor_variants(release_id(), spark_jar())
        no_dup_snv = qa.no_dup_snv(release_id(), spark_jar())
        no_dup_consequences = qa.no_dup_consequences(release_id(), spark_jar())
        no_dup_variants = qa.no_dup_variants(release_id(), spark_jar())
        no_dup_variant_centric = qa.no_dup_variant_centric(release_id(), spark_jar())
        no_dup_cnv_centric = qa.no_dup_cnv_centric(release_id(), spark_jar())
        same_list_nor_snv_nor_variants = qa.same_list_nor_snv_nor_variants(release_id(), spark_jar())
        same_list_snv_variants = qa.same_list_snv_variants(release_id(), spark_jar())
        same_list_variants_variant_centric = qa.same_list_variants_variant_centric(release_id(), spark_jar())

        [non_empty_tables, no_dup_gnomad, no_dup_nor_snv, no_dup_nor_consequences, no_dup_nor_variants, no_dup_snv,
         no_dup_consequences, no_dup_variants, no_dup_variant_centric, no_dup_cnv_centric,
         same_list_nor_snv_nor_variants, same_list_snv_variants, same_list_variants_variant_centric]


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

    params_validate_task >> ingest_germline_group >> enrich_germline() >> prepare_group >> qa_germline() >> index_group >> publish_group >> notify_task
