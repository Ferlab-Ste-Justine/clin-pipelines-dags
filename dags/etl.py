from datetime import datetime
from typing import List, Dict

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.config import Env, K8sContext, env
from lib.groups.index.get_release_ids import get_release_ids
from lib.groups.index.index import index
from lib.groups.index.prepare_index import prepare_index
from lib.groups.index.publish_index import publish_index
from lib.groups.qa import qa
from lib.operators.notify import NotifyOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks import batch_type, clinical, enrich, es, params
from lib.tasks.batch_type import skip_if_no_batch_in
from lib.tasks.params_validate import validate_color
from lib.utils_etl import (ClinAnalysis, color, default_or_initial,
                           get_ingest_dag_configs_by_analysis_ids,
                           get_ingest_dag_configs_by_batch_id, release_id,
                           skip_notify, spark_jar)

with DAG(
        dag_id='etl',
        start_date=datetime(2022, 1, 1),
        schedule='0 12 * * 6' if env == Env.QA else None,  # every Saturday at 12:00pm
        catchup=False,
        params={
            'batch_ids': Param([], type=['null', 'array'],
                               description='Put a single batch id per line. Leave empty to skip ingest.'),
            'analysis_ids': Param([], type=['null', 'array'],
                                  description='Put a single id per line. Leave empty to skip ingest.'),
            'release_id': Param('', type=['null', 'string']),
            'color': Param('', type=['null', 'string']),
            'import': Param('no', enum=['yes', 'no']),
            'cnv_frequencies': Param('yes', enum=['yes', 'no']),
            'delete_previous_releases': Param('yes', enum=['yes', 'no']),
            'notify': Param('no', enum=['yes', 'no']),
            'qc': Param('yes', enum=['yes', 'no']),
            'rolling': Param('yes' if env == Env.QA else 'no', enum=['yes', 'no']),
            'spark_jar': Param('', type=['null', 'string']),
        },
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_tasks=4,
        max_active_runs=1,
        render_template_as_native_obj=True,
        user_defined_macros={'any_in': batch_type.any_in}
) as dag:
    def skip_qc() -> str:
        return '{% if params.qc == "yes" %}{% else %}yes{% endif %}'

    def skip_cnv_frequencies() -> str:
        return '{% if params.cnv_frequencies == "yes" %}{% else %}yes{% endif %}'

    def skip_if_cnv_frequencies() -> str:
        return '{% if params.cnv_frequencies == "yes" %}yes{% else %}{% endif %}'

    def skip_delete_previous_releases() -> str:
        return '{% if params.delete_previous_releases == "yes" %}{% else %}yes{% endif %}'

    def skip_rolling() -> str:
        if env != Env.QA:
            return 'yes'
        else:
            return '{% if params.rolling == "yes" %}{% else %}yes{% endif %}'


    params_validate_task = validate_color(color=color())

    env_color = params_validate_task.__str__() if env == Env.QA else None
    underscore_color = ('_' + env_color) if env_color else ''
    dash_color = ('-' + env_color) if env_color else ''

    get_batch_ids_task = params.get_batch_ids()
    get_analysis_ids_task = params.get_analysis_ids()

    detect_batch_types_task = batch_type.detect(batch_ids=get_batch_ids_task, analysis_ids=get_analysis_ids_task, allowMultipleIdentifierTypes=True)

    get_ingest_dag_configs_by_batch_id_task = get_ingest_dag_configs_by_batch_id.expand(batch_id=get_batch_ids_task)
    get_ingest_dag_configs_by_analysis_ids_task = get_ingest_dag_configs_by_analysis_ids.partial(all_batch_types=detect_batch_types_task, analysis_ids=get_analysis_ids_task).expand(analysisType=[ClinAnalysis.GERMLINE.value, ClinAnalysis.SOMATIC_TUMOR_ONLY.value])

    trigger_ingest_by_batch_id_dags = TriggerDagRunOperator.partial(
        task_id='ingest_batches',
        trigger_dag_id='etl_ingest',
        wait_for_completion=True
    ).expand(conf=get_ingest_dag_configs_by_batch_id_task)

    trigger_ingest_by_analysis_ids_dags = TriggerDagRunOperator.partial(
        task_id='ingest_analysis_ids',
        trigger_dag_id='etl_ingest',
        wait_for_completion=True,
    ).expand(conf=get_ingest_dag_configs_by_analysis_ids_task)

    steps = default_or_initial(batch_param_name='batch_ids', analysis_param_name='analysis_ids')

    @task_group(group_id='enrich')
    def enrich_group():
        @task(task_id='get_analysis_and_batch_for_types', multiple_outputs=True)
        def get_analysis_and_batch_for_types(batch_ids: List[str], analysis_ids: List[str], all_batch_types: Dict[str, str], target_batch_types: List[ClinAnalysis]) -> dict:
            if not (batch_ids or analysis_ids):
                raise AirflowSkipException("Skipping get_analysis_and_batch_for_types task")

            filtered_batch_ids = [b for b in batch_ids if ClinAnalysis(all_batch_types.get(b)) in target_batch_types]
            filtered_analysis_ids = [a for a in analysis_ids if ClinAnalysis(all_batch_types.get(a)) in target_batch_types]
            return {"batch_ids": filtered_batch_ids, "analysis_ids": filtered_analysis_ids}


        # Only run snv if at least one germline batch
        snv = enrich.snv(
            steps=steps,
            spark_jar=spark_jar(),
            skip=skip_if_no_batch_in(target_batch_types=[ClinAnalysis.GERMLINE])
        )

        @task_group(group_id='snv_somatic')
        def snv_somatic_group():
            # Only run snv_somatic if at least one somatic tumor only or somatic tumor normal batch
            # Run snv_somatic_all if no batch ids or analysis ids are provided. Otherwise, run snv_somatic on applicable analysis ids.

            snv_somatic_target_types = [ClinAnalysis.SOMATIC_TUMOR_ONLY, ClinAnalysis.SOMATIC_TUMOR_NORMAL]
            get_snv_somatic_analysis_and_batch_task = get_analysis_and_batch_for_types(
                batch_ids=get_batch_ids_task,
                analysis_ids=get_analysis_ids_task,
                all_batch_types=detect_batch_types_task,
                target_batch_types=snv_somatic_target_types
            )
            get_snv_somatic_analysis_ids_task = clinical.get_all_analysis_ids(
                batch_ids=get_snv_somatic_analysis_and_batch_task['batch_ids'],
                analysis_ids=get_snv_somatic_analysis_and_batch_task['analysis_ids']
            )

            @task.branch(task_id='run_snv_somatic')
            def run_snv_somatic(batch_ids: List[str], analysis_ids: List[str]) -> str:
                if not (batch_ids or analysis_ids):
                    return 'enrich.snv_somatic.snv_somatic_all'
                else:
                    return 'enrich.snv_somatic.snv_somatic'

            run_snv_somatic_task = run_snv_somatic(batch_ids=get_batch_ids_task, analysis_ids=get_analysis_ids_task)

            snv_somatic_all = enrich.snv_somatic_all(
                spark_jar=spark_jar(),
                steps=steps
            )

            snv_somatic = enrich.snv_somatic(
                analysis_ids=get_snv_somatic_analysis_ids_task,
                spark_jar=spark_jar(),
                steps=steps,
                skip=skip_if_no_batch_in(target_batch_types=snv_somatic_target_types)
            )

            get_snv_somatic_analysis_and_batch_task >> get_snv_somatic_analysis_ids_task >> run_snv_somatic_task >> [snv_somatic_all, snv_somatic]

        @task_group(group_id='cnv')
        def cnv_group():
            # Only run cnv if at least one germline or somatic tumor only batch
            # Run cnv_all if no batch ids or analysis ids are provided. Otherwise, run cnv on applicable analysis ids.

            cnv_target_types = [ClinAnalysis.GERMLINE, ClinAnalysis.SOMATIC_TUMOR_ONLY]
            get_cnv_analysis_and_batch_task = get_analysis_and_batch_for_types(
                batch_ids=get_batch_ids_task,
                analysis_ids=get_analysis_ids_task,
                all_batch_types=detect_batch_types_task,
                target_batch_types=cnv_target_types
            )
            get_cnv_analysis_ids_task = clinical.get_all_analysis_ids(
                batch_ids=get_cnv_analysis_and_batch_task['batch_ids'],
                analysis_ids=get_cnv_analysis_and_batch_task['analysis_ids']
            )

            @task.branch(task_id='run_cnv')
            def run_cnv(batch_ids: List[str], analysis_ids: List[str]) -> str:
                if not (batch_ids or analysis_ids) or env == Env.PROD:
                    return 'enrich.cnv.cnv_all'
                else:
                    return 'enrich.cnv.cnv'  # too complex for PROD, cnv_all works fine (for now)

            run_cnv_task = run_cnv(batch_ids=get_batch_ids_task, analysis_ids=get_analysis_ids_task)
            cnv_all = enrich.cnv_all(spark_jar=spark_jar(), steps=steps, skip=skip_if_cnv_frequencies())
            cnv = enrich.cnv(
                analysis_ids=get_cnv_analysis_ids_task,
                spark_jar=spark_jar(),
                steps=steps,
                skip=skip_if_no_batch_in(cnv_target_types) + skip_if_cnv_frequencies(),
            )

            get_cnv_analysis_and_batch_task >> get_cnv_analysis_ids_task >> run_cnv_task >> [cnv_all, cnv]

        # Always run variants, consequences and coverage by gene
        variants = enrich.variants(spark_jar=spark_jar(), steps=steps)
        consequences = enrich.consequences(spark_jar=spark_jar(), steps=steps)
        coverage_by_gene = enrich.coverage_by_gene(spark_jar=spark_jar(), steps=steps)

        snv >> snv_somatic_group() >> variants >> consequences >> cnv_group() >> coverage_by_gene


    prepare_group = prepare_index(
        spark_jar=spark_jar(),
        skip_cnv_centric=skip_if_no_batch_in(target_batch_types=[ClinAnalysis.GERMLINE,
                                                                 ClinAnalysis.SOMATIC_TUMOR_ONLY]) + skip_if_cnv_frequencies()
    )

    qa_group = qa(
        spark_jar=spark_jar()
    )

    get_release_ids_group = get_release_ids(
        release_id=release_id(),
        color=underscore_color,
        increment_release_id=True,  # Get new release ID
        skip_cnv_centric=skip_if_no_batch_in(target_batch_types=[ClinAnalysis.GERMLINE,
                                                                 ClinAnalysis.SOMATIC_TUMOR_ONLY]) + skip_if_cnv_frequencies()
    )


    @task_group(group_id='delete_previous_variant_centric')
    def delete_previous_variant_centric_group():

        @task(task_id='get_previous_variant_centric_release')
        def get_previous_variant_centric_release(release_id: str) -> str:
            return es.get_previous_release(release_id)

        get_previous_variant_centric_release_task = get_previous_variant_centric_release(release_id('variant_centric'))

        # for PROD mostly, cause variant_centric is the bigger disk consumer
        delete_previous_variant_centric_index = TriggerDagRunOperator(
            task_id='delete_previous_variant_centric_index',
            trigger_dag_id='etl_es_utils',
            wait_for_completion=True,
            skip=skip_delete_previous_releases(),
            conf={
                "delete_release": "no",
                "delete_variant_release": "yes",
                "test_duplicated_variants": "no",
                "show_indexes": "yes",
                "test_disk_usage": "yes",
                "release_id": get_previous_variant_centric_release_task,
                "color": env_color
            }
        )

        get_previous_variant_centric_release_task >> delete_previous_variant_centric_index

    index_group = index(
        color=underscore_color,
        spark_jar=spark_jar(),
        skip_cnv_centric=skip_if_no_batch_in(target_batch_types=[ClinAnalysis.GERMLINE,
                                                                 ClinAnalysis.SOMATIC_TUMOR_ONLY]) + skip_if_cnv_frequencies()
    )

    publish_group = publish_index(
        color=underscore_color,
        spark_jar=spark_jar(),
        skip_cnv_centric=skip_if_no_batch_in(target_batch_types=[ClinAnalysis.GERMLINE,
                                                                 ClinAnalysis.SOMATIC_TUMOR_ONLY]) + skip_if_cnv_frequencies()
    )

    # Use operator directly for dynamic task mapping
    notify_task = NotifyOperator.partial(
        task_id='notify',
        name='notify',
        k8s_context=K8sContext.DEFAULT,
        color=env_color,
        skip=skip_notify(batch_param_name='batch_ids')
    ).expand(
        batch_id=get_batch_ids_task
    )

    trigger_qc_es_dag = TriggerDagRunOperator(
        task_id='qc_es',
        trigger_dag_id='etl_qc_es',
        wait_for_completion=True,
        skip=skip_qc(),
    )

    trigger_qc_dag = TriggerDagRunOperator(
        task_id='qc',
        trigger_dag_id='etl_qc',
        wait_for_completion=True,
        skip=skip_qc(),
        conf={
            'spark_jar': spark_jar()
        }
    )

    trigger_rolling_dag = TriggerDagRunOperator(
        task_id='rolling',
        trigger_dag_id='etl_rolling',
        wait_for_completion=True,
        skip=skip_rolling(),
        conf={
            'gene_centric_release_id': release_id('gene_centric'),
            'gene_suggestions_release_id': release_id('gene_suggestions'),
            'variant_centric_release_id': release_id('variant_centric'),
            'variant_suggestions_release_id': release_id('variant_suggestions'),
            'coverage_by_gene_centric_release_id': release_id('coverage_by_gene_centric'),
            'cnv_centric_release_id': release_id('cnv_centric'),
            'color': env_color
        }
    )

    trigger_delete_previous_releases = TriggerDagRunOperator(
        task_id='delete_previous_releases',
        trigger_dag_id='etl_delete_previous_releases',
        wait_for_completion=True,
        skip=skip_delete_previous_releases(),
        conf={
            'release_id': release_id(),
            'color': env_color,
        }
    )

    trigger_cnv_frequencies = TriggerDagRunOperator(
        task_id='cnv_frequencies',
        trigger_dag_id='etl_cnv_frequencies',
        wait_for_completion=True,
        skip=skip_cnv_frequencies(),
        conf={
            'color': env_color,
            'spark_jar': spark_jar()
        }
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    (params_validate_task >> [get_batch_ids_task >> get_analysis_ids_task] >> detect_batch_types_task >>
     [get_ingest_dag_configs_by_batch_id_task >> get_ingest_dag_configs_by_analysis_ids_task] >>
     trigger_ingest_by_batch_id_dags >> trigger_ingest_by_analysis_ids_dags >> enrich_group() >> prepare_group >> qa_group >> get_release_ids_group >>
     delete_previous_variant_centric_group() >> index_group >>
     publish_group >> trigger_rolling_dag >> trigger_delete_previous_releases >> trigger_cnv_frequencies >> notify_task >> slack >> trigger_qc_es_dag >> trigger_qc_dag)
