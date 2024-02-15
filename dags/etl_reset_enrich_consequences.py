from datetime import datetime

from airflow import DAG

from lib.slack import Slack
from lib.tasks import enrich

with DAG(
        dag_id='etl_reset_enrich_consequences',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
) as dag:
    enrich.consequences(
        steps='initial',
        task_id='reset_enrich_consequences',
        name='etl-reset-enrich-consequences',
        app_name='reset_enrich_consequences',
        on_execute_callback=Slack.notify_dag_start,
        on_success_callback=Slack.notify_dag_completion
    )
