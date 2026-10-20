from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from lib.slack import Slack
from lib.tasks import enrich
from lib.utils_etl import spark_jar

with DAG(
        dag_id='etl_reset_enrich_variants',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
        params={
            'spark_jar': Param('', type=['null', 'string']),
        },
) as dag:
    enrich.variants(
        steps='initial',
        spark_jar=spark_jar(),
        task_id='reset_enrich_variants',
        name='etl-reset-enrich-variants',
        app_name='reset_enrich_variants',
        on_execute_callback=Slack.notify_dag_start,
        on_success_callback=Slack.notify_dag_completion
    )
