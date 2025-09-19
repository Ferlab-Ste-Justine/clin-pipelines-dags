from datetime import datetime

import pendulum
from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule
from lib.config import Env, env
from lib.config_nextflow import (nextflow_svclustering_germline_input_key,
                                 nextflow_svclustering_germline_output_key,
                                 nextflow_svclustering_somatic_input_key,
                                 nextflow_svclustering_somatic_output_key)
from lib.doc import cnv_frequencies as doc
from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks import (enrich, es, index, params_validate, prepare_index,
                       publish_index, qa)
from lib.tasks.nextflow import svclustering
from lib.utils_etl import color, release_id, spark_jar

with DAG(
        dag_id='etl_cnv_frequencies',
        doc_md=doc.cnv_frequencies,
        start_date=datetime(2024, 10, 20, 2, tzinfo=pendulum.timezone("America/Montreal")),
        schedule=None,
        params={
            'release_id': Param('', type=['null', 'string']),
            'color': Param('', type=['null', 'string']),
            'spark_jar': Param('', type=['null', 'string']),
        },
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure
        },
        catchup=False,
        max_active_runs=1,
        max_active_tasks=2
) as dag:
    params_validate_task = params_validate.validate_color(color=color())

    ingest_fhir_group = ingest_fhir(
        batch_ids=[],  # No associated "batch"
        color=color(),
        skip_all=False,
        skip_import=True,  # Skipping because we only want to run enrich_clinical
        skip_batch=False,
        spark_jar=spark_jar()
    )

    prepare_svclustering_task = svclustering.prepare()


    @task_group(group_id='run')
    def run_group():
        svclustering.run(input_key=nextflow_svclustering_germline_input_key,
                         output_key=nextflow_svclustering_germline_output_key,
                         task_id='svclustering_germline',
                         name='svclustering-germline')

        svclustering.run(input_key=nextflow_svclustering_somatic_input_key,
                         output_key=nextflow_svclustering_somatic_output_key,
                         task_id='svclustering_somatic',
                         name='svclustering-somatic')


    @task_group(group_id='normalize')
    def normalize_group():
        svclustering.normalize(entrypoint='normalize_svclustering_germline_del',
                               spark_jar=spark_jar(),
                               task_id='svclustering_germline_del',
                               name='svclustering-germline-del')
        svclustering.normalize(entrypoint='normalize_svclustering_germline_dup',
                               spark_jar=spark_jar(),
                               task_id='svclustering_germline_dup',
                               name='svclustering-germline-dup')

        svclustering.normalize(entrypoint='normalize_svclustering_somatic_del',
                               spark_jar=spark_jar(),
                               task_id='svclustering_somatic_del',
                               name='svclustering-somatic-del')
        svclustering.normalize(entrypoint='normalize_svclustering_somatic_dup',
                               spark_jar=spark_jar(),
                               task_id='svclustering_somatic_dup',
                               name='svclustering-somatic-dup')


    enrich_cnv_task = enrich.cnv_all(spark_jar=spark_jar(), steps='default', task_id='enrich_cnv')
    prepare_cnv_centric_task = prepare_index.cnv_centric(spark_jar(), task_id='prepare_cnv_centric')


    @task_group(group_id='qa')
    def qa_group():
        no_dup_nextflow_svclustering_germline_task = qa.no_dup_nextflow_svclustering_germline(spark_jar())
        no_dup_nextflow_svclustering_somatic_task = qa.no_dup_nextflow_svclustering_somatic(spark_jar())
        no_dup_nextflow_svclustering_parental_origin_task = qa.no_dup_nextflow_svclustering_parental_origin(spark_jar())
        no_dup_cnv_centric_task = qa.no_dup_cnv_centric(spark_jar())
        [no_dup_nextflow_svclustering_germline_task, no_dup_nextflow_svclustering_somatic_task,
         no_dup_nextflow_svclustering_parental_origin_task, no_dup_cnv_centric_task]


    release_id = es.get_release_id(release_id(), color('_'), index='cnv_centric')
    index_cnv_centric_task = index.cnv_centric(release_id, color('_'), spark_jar(), task_id='index_cnv_centric')
    publish_cnv_centric_task = publish_index.cnv_centric(release_id, color('_'), spark_jar(),
                                                         task_id='publish_cnv_centric',
                                                         on_success_callback=Slack.notify_dag_completion)
    delete_previous_release_task = es.delete_previous_release('cnv_centric', release_id, color('_'))

    trigger_rolling_dag = TriggerDagRunOperator(
        task_id='rolling',
        trigger_dag_id='etl_rolling',
        wait_for_completion=True,
        skip='yes' if env != Env.QA else '',
        conf={
            'color': color()
        }
    )

    (params_validate_task >> ingest_fhir_group >> prepare_svclustering_task >> run_group() >> normalize_group() >>
     enrich_cnv_task >> prepare_cnv_centric_task >> qa_group() >> index_cnv_centric_task >> publish_cnv_centric_task >>
     trigger_rolling_dag >> delete_previous_release_task)
