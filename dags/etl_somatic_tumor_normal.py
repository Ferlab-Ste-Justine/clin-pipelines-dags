from datetime import datetime

from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

from etl_qa import release_id, spark_jar
from lib.groups.ingest.ingest_somatic_tumor_normal import ingest_somatic_tumor_normal
from lib.groups.qa import qa
from lib.slack import Slack
from lib.tasks import (arranger, enrich, index, prepare_index as prepare, publish_index as publish)
from lib.tasks.notify import notify
from lib.tasks.params_validate import validate_release_color
from lib.utils_etl import color, batch_id, skip_import, skip_batch, default_or_initial, skip_notify

with DAG(
        dag_id='etl_somatic_tumor_normal',
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

    ingest_somatic_tumor_normal_group = ingest_somatic_tumor_normal(
        batch_id=batch_id(),
        color=color(),
        skip_import=skip_import(),
        skip_batch=skip_batch(),
        skip_snv_somatic=skip_batch(),
        skip_variants=skip_batch(),
        skip_consequences=skip_batch(),
        skip_coverage_by_gene=skip_batch(),
        spark_jar=spark_jar()
    )


    @task_group(group_id='enrich')
    def enrich_somatic_tumor_normal():
        """
        Run all enrich tasks except cnv.
        """
        snv_somatic = enrich.snv_somatic(spark_jar=spark_jar(), steps=default_or_initial())
        variants = enrich.variants(spark_jar=spark_jar(), steps=default_or_initial())
        consequences = enrich.consequences(spark_jar=spark_jar(), steps=default_or_initial())
        coverage_by_gene = enrich.coverage_by_gene(spark_jar=spark_jar(), steps=default_or_initial())

        snv_somatic >> variants >> consequences >> coverage_by_gene


    @task_group(group_id='prepare')
    def prepare_index_somatic_tumor_normal():
        """
        Run all prepare index tasks except cnv_centric.
        """
        gene_centric = prepare.gene_centric(spark_jar())
        gene_suggestions = prepare.gene_suggestions(spark_jar())
        variant_centric = prepare.variant_centric(spark_jar())
        variant_suggestions = prepare.variant_suggestions(spark_jar())
        coverage_by_gene_centric = prepare.coverage_by_gene_centric(spark_jar())

        gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> coverage_by_gene_centric


    qa_group = qa(
        release_id=release_id(),
        spark_jar=spark_jar()
    )


    @task_group(group_id='index')
    def index_somatic_tumor_normal():
        """
        Run all index tasks except cnv_centric.
        """
        gene_centric = index.gene_centric(release_id(), color(), spark_jar())
        gene_suggestions = index.gene_suggestions(release_id(), color(), spark_jar())
        variant_centric = index.variant_centric(release_id(), color(), spark_jar())
        variant_suggestions = index.variant_suggestions(release_id(), color(), spark_jar())
        coverage_by_gene_centric = index.coverage_by_gene_centric(release_id(), color(), spark_jar())

        [gene_centric, gene_suggestions] >> variant_centric >> [variant_suggestions, coverage_by_gene_centric]


    @task_group(group_id='publish')
    def publish_index_somatic_tumor_normal():
        """
        Run all publish index tasks except cnv centric.
        """
        gene_centric = publish.gene_centric(release_id(), color(), spark_jar())
        gene_suggestions = publish.gene_suggestions(release_id(), color(), spark_jar())
        variant_centric = publish.variant_centric(release_id(), color(), spark_jar())
        variant_suggestions = publish.variant_suggestions(release_id(), color(), spark_jar())
        coverage_by_gene_centric = publish.coverage_by_gene_centric(release_id(), color(), spark_jar())

        arranger_remove_project_task = arranger.remove_project()
        arranger_restart_task = arranger.restart(on_success_callback=Slack.notify_dag_completion)

        [gene_centric, gene_suggestions, variant_centric, variant_suggestions,
         coverage_by_gene_centric] >> arranger_remove_project_task >> arranger_restart_task


    notify_task = notify(
        batch_id=batch_id(),
        color=color(),
        skip=skip_notify()
    )

    params_validate_task >> ingest_somatic_tumor_normal_group >> enrich_somatic_tumor_normal() >> prepare_index_somatic_tumor_normal() >> qa_group >> index_somatic_tumor_normal() >> publish_index_somatic_tumor_normal() >> notify_task
