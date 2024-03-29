from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import DagRun
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.config import K8sContext
from lib.groups.index.index import index
from lib.groups.index.prepare_index import prepare_index
from lib.groups.index.publish_index import publish_index
from lib.groups.qa import qa
from lib.operators.notify import NotifyOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks import batch_type, enrich
from lib.tasks.params_validate import validate_release_color
from lib.utils_etl import (release_id, color, spark_jar, default_or_initial,
                           ClinAnalysis, skip_notify)


def any_in(values, iterable):
    """
    Macro for Jinja templating.
    """
    return any(value in iterable for value in values)


with DAG(
        dag_id='etl',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'batch_ids': Param([], type=['null', 'array'],
                               description='Put a single batch id per line. Leave empty to skip ingest.'),
            'release_id': Param('', type='string'),
            'color': Param('', type=['null', 'string']),
            'import': Param('yes', enum=['yes', 'no']),
            'notify': Param('no', enum=['yes', 'no']),
            'qc': Param('yes', enum=['yes', 'no']),
            'rolling': Param('no', enum=['yes', 'no']),
            'spark_jar': Param('', type=['null', 'string']),
        },
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_tasks=4,
        render_template_as_native_obj=True,
        user_defined_macros={'any_in': any_in}
) as dag:
    def skip_if_no_target_batches(target_batch_types: List[ClinAnalysis]) -> str:
        """
        Checks if at least one current batch type matches at least one of the target batch types. If there is a single
        match or if no batch_ids were passed, returns False so task won't be skipped. Otherwise, if there are no matches,
        returns True so task will be skipped.

        To use, pass macro any_in as user_defined_macros in DAG definition.
        """
        return f"{{% set targets = {[target.value for target in target_batch_types]} %}}" \
               "{% set batch_types = task_instance.xcom_pull(task_ids='detect_batch_type') %}" \
               "{% if not batch_types or any_in(targets, batch_types) %}{% else %}'yes'{% endif %}"


    def skip_qc() -> str:
        return '{% if params.qc == "yes" %}{% else %}yes{% endif %}'


    def skip_rolling() -> str:
        return '{% if params.rolling == "yes" %}{% else %}yes{% endif %}'


    params_validate_task = validate_release_color.override(on_execute_callback=Slack.notify_dag_start)(
        release_id=release_id(),
        color=color()
    )


    @task(task_id='get_batch_ids')
    def get_batch_ids(ti=None) -> List[str]:
        dag_run: DagRun = ti.dag_run
        return dag_run.conf['batch_ids'] if dag_run.conf['batch_ids'] is not None else []


    @task(task_id='get_ingest_dag_configs')
    def get_ingest_dag_config(batch_id: str, ti=None) -> dict:
        dag_run: DagRun = ti.dag_run
        return {
            'batch_id': batch_id,
            'color': dag_run.conf['color'],
            'import': dag_run.conf['import'],
            'spark_jar': dag_run.conf['spark_jar']
        }


    get_batch_ids_task = get_batch_ids()
    detect_batch_types_task = batch_type.detect.expand(batch_id=get_batch_ids_task)
    get_ingest_dag_configs_task = get_ingest_dag_config.expand(batch_id=get_batch_ids_task)

    trigger_ingest_dags = TriggerDagRunOperator.partial(
        task_id='ingest_batches',
        trigger_dag_id='etl_ingest',
        wait_for_completion=True
    ).expand(conf=get_ingest_dag_configs_task)

    steps = default_or_initial(batch_param_name='batch_ids')


    @task_group(group_id='enrich')
    def enrich_group():
        # Only run snv if at least one germline batch
        snv = enrich.snv(
            steps=steps,
            spark_jar=spark_jar(),
            skip=skip_if_no_target_batches(target_batch_types=[ClinAnalysis.GERMLINE])
        )

        # Only run snv_somatic if at least one somatic tumor only or somatic tumor normal batch
        # Run snv_somatic_all if no batch ids are provided. Otherwise, run snv_somatic for each batch_id.
        @task.branch(task_id='run_snv_somatic')
        def run_snv_somatic(batch_ids: List[str]):
            if not batch_ids:
                return 'enrich.snv_somatic_all'
            else:
                return 'enrich.snv_somatic'

        run_snv_somatic_task = run_snv_somatic(batch_ids=get_batch_ids_task)

        snv_somatic_all = enrich.snv_somatic_all(
            spark_jar=spark_jar(),
            steps=steps,
            skip=skip_if_no_target_batches(target_batch_types=[ClinAnalysis.SOMATIC_TUMOR_ONLY,
                                                               ClinAnalysis.SOMATIC_TUMOR_NORMAL])
        )

        snv_somatic = enrich.snv_somatic(
            batch_ids=get_batch_ids_task,
            spark_jar=spark_jar(),
            steps=steps,
            skip=skip_if_no_target_batches(target_batch_types=[ClinAnalysis.SOMATIC_TUMOR_ONLY,
                                                               ClinAnalysis.SOMATIC_TUMOR_NORMAL])
        )

        # Only run if at least one germline or somatic tumor only batch
        cnv = enrich.cnv(
            spark_jar=spark_jar(),
            steps=steps,
            skip=skip_if_no_target_batches(target_batch_types=[ClinAnalysis.GERMLINE,
                                                               ClinAnalysis.SOMATIC_TUMOR_ONLY])
        )

        # Always run variants, consequences and coverage by gene
        variants = enrich.variants(spark_jar=spark_jar(), steps=steps)
        consequences = enrich.consequences(spark_jar=spark_jar(), steps=steps)
        coverage_by_gene = enrich.coverage_by_gene(spark_jar=spark_jar(), steps=steps)

        snv >> run_snv_somatic_task >> [snv_somatic_all,
                                        snv_somatic] >> variants >> consequences >> cnv >> coverage_by_gene


    prepare_group = prepare_index(
        spark_jar=spark_jar(),
        skip_cnv_centric=skip_if_no_target_batches(target_batch_types=[ClinAnalysis.GERMLINE,
                                                                       ClinAnalysis.SOMATIC_TUMOR_ONLY])
    )

    qa_group = qa(
        release_id=release_id(),
        spark_jar=spark_jar()
    )

    index_group = index(
        release_id=release_id(),
        color=color('_'),
        spark_jar=spark_jar(),
        skip_cnv_centric=skip_if_no_target_batches(target_batch_types=[ClinAnalysis.GERMLINE,
                                                                       ClinAnalysis.SOMATIC_TUMOR_ONLY])
    )

    publish_group = publish_index(
        release_id=release_id(),
        color=color('_'),
        spark_jar=spark_jar(),
        skip_cnv_centric=skip_if_no_target_batches(target_batch_types=[ClinAnalysis.GERMLINE,
                                                                       ClinAnalysis.SOMATIC_TUMOR_ONLY])
    )

    # Use operator directly for dynamic task mapping
    notify_task = NotifyOperator.partial(
        task_id='notify',
        name='notify',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        skip=skip_notify(batch_param_name='batch_ids')
    ).expand(
        batch_id=get_batch_ids_task
    )

    trigger_qc_dag = TriggerDagRunOperator(
        task_id='qc',
        trigger_dag_id='etl_qc',
        wait_for_completion=True,
        skip=skip_qc(),
        conf={
            'release_id': release_id(),
            'spark_jar': spark_jar()
        }
    )

    trigger_rolling_dag = TriggerDagRunOperator(
        task_id='rolling',
        trigger_dag_id='etl_rolling',
        wait_for_completion=True,
        skip=skip_rolling(),
        conf={
            'release_id': release_id(),
            'color': color()
        }
    )

    params_validate_task >> get_batch_ids_task >> detect_batch_types_task >> get_ingest_dag_configs_task >> trigger_ingest_dags >> enrich_group() >> prepare_group >> qa_group >> index_group >> publish_group >> notify_task >> trigger_qc_dag >> trigger_rolling_dag
