from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task_group
from airflow.models import Param
from lib.doc import cnv_frequencies as doc
from lib.slack import Slack
from lib.tasks import (enrich, es, index, params_validate,
                       prepare_index, publish_index, qa)
from lib.tasks.nextflow import svclustering
from lib.utils_etl import color, release_id, spark_jar

with DAG(
        dag_id='etl_cnv_frequencies',
        doc_md=doc.cnv_frequencies,
        start_date=datetime(2024, 10, 20, 2, tzinfo=pendulum.timezone("America/Montreal")),
        schedule_interval=None,
        params={
            'release_id': Param('', type=['null', 'string']),
            'color': Param('', type=['null', 'string']),
            'spark_jar': Param('', type=['null', 'string']),
        },
        default_args={
            'on_failure_callback': Slack.notify_task_failure
        },
        catchup=False,
        max_active_runs=1
) as dag:
    params_validate_task = params_validate.validate_color(color=color())
    prepare_svclustering_task = svclustering.prepare(spark_jar())
    run_svclustering_task = svclustering.run()
    normalize_svclustering_task = svclustering.normalize(spark_jar())
    enrich_cnv_task = enrich.cnv_all(spark_jar=spark_jar(), steps='initial', task_id='enrich_cnv')
    prepare_cnv_centric_task = prepare_index.cnv_centric(spark_jar(), task_id='prepare_cnv_centric')


    @task_group(group_id='qa')
    def qa_group():
        no_dup_nextflow_svclustering_task = qa.no_dup_nextflow_svclustering(spark_jar())
        no_dup_nextflow_svclustering_parental_origin_task = qa.no_dup_nextflow_svclustering_parental_origin(spark_jar())
        no_dup_cnv_centric_task = qa.no_dup_cnv_centric(spark_jar())
        [no_dup_nextflow_svclustering_task, no_dup_nextflow_svclustering_parental_origin_task, no_dup_cnv_centric_task]


    release_id = es.get_release_id(release_id(), color('_'), index='cnv_centric')
    index_cnv_centric_task = index.cnv_centric(release_id, color('_'), spark_jar(), task_id='index_cnv_centric')
    publish_cnv_centric_task = publish_index.cnv_centric(release_id, color('_'), spark_jar(),
                                                         task_id='publish_cnv_centric',
                                                         on_success_callback=Slack.notify_dag_completion)
    delete_previous_release_task = es.delete_previous_release('cnv_centric', release_id, color('_'))

    (params_validate_task >> prepare_svclustering_task >> run_svclustering_task >> normalize_svclustering_task >>
     enrich_cnv_task >> prepare_cnv_centric_task >> qa_group() >> index_cnv_centric_task >> publish_cnv_centric_task >> delete_previous_release_task)
