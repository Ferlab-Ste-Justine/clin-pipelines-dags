import logging
from datetime import datetime
from typing import List

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.groups.es import es
from lib.slack import Slack

with DAG(
    dag_id='etl_run',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    params={
        'sequencing_ids': Param([], type=['null', 'array']),
    },
    render_template_as_native_obj=True,
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
    max_active_tasks=1,
    max_active_runs=1
) as dag:

    def sequencing_ids():
        return '{{ params.sequencing_ids }}'

    def _run(sequencing_ids: List[str]):
        logging.info(f'Run ETLs for total: {len(sequencing_ids)} sequencing_ids: {sequencing_ids}')

    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )

    run_etl = PythonOperator(
        task_id='run',
        op_args=[sequencing_ids()],
        python_callable=_run,
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> run_etl >> slack
