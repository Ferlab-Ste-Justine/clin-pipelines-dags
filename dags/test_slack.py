from airflow import DAG
from datetime import datetime
from lib import config
from lib.operators.slack import SlackOperator
from lib.slack import Slack


if (config.show_test_dags):

    with DAG(
        dag_id='test_slack',
        start_date=datetime(2022, 1, 1),
        schedule=None,
    ) as dag:

        test_slack = SlackOperator(
            task_id='test_slack',
            markdown='This is a test notification.',
            on_execute_callback=Slack.notify_dag_start,
            on_success_callback=Slack.notify_dag_completion,
        )
