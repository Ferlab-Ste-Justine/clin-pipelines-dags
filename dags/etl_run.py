from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.slack import Slack
from lib.tasks.params import get_sequencing_ids
from lib.tasks.run_sequencings import save_sequencing_ids_to_s3

with DAG(
        dag_id='etl_run',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        catchup=False,
        params={
            'sequencing_ids': Param([], type=['null', 'array']),
            'spark_jar': Param('', type=['null', 'string']),
        },
        render_template_as_native_obj=True,
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_runs=10 # Allow multiple concurrent runs, we are just writing files to S3
) as dag:

    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )
    
    save_task = save_sequencing_ids_to_s3(get_sequencing_ids())

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> save_task >> slack
