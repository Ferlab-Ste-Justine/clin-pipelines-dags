from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from lib.slack import Slack
from lib.tasks.notify import notify
from lib.tasks.params_validate import validate_batch_color
from lib.utils_etl import color, batch_id

with DAG(
        dag_id='etl_notify',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'batch_id': Param('', type='string'),
            'color': Param('', type=['null', 'string']),
        },
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
) as dag:
    params_validate = validate_batch_color(batch_id(), color())

    notify_task = notify(
        batch_id=batch_id(),
        color=color(),
        skip=''  # Don't skip -- purpose of this DAG
    )

    params_validate >> notify_task
