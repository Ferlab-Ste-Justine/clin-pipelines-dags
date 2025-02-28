from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.groups.es import es
from lib.slack import Slack

with DAG(
    dag_id='etl_qc_es',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
        'trigger_rule': TriggerRule.NONE_FAILED,
    },
    max_active_tasks=1
) as dag:

    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> es() >> slack
