from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.doc import franklin as doc
from lib.groups.franklin.franklin_create import FranklinCreate
from lib.groups.franklin.franklin_update import FranklinUpdate
from lib.slack import Slack

with DAG(
        dag_id='etl_import_franklin',
        doc_md=doc.franklin,
        start_date=datetime(2022, 1, 1),
        schedule=None,
        render_template_as_native_obj=True,
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        params={
            'batch_id': Param('', type='string'),
        },
) as dag:

    def batch_id() -> str:
        return '{{ params.batch_id or "" }}'

    def validate_params(batch_id):
        if batch_id == '':
            raise AirflowFailException('DAG param "batch_id" is required')

    params = PythonOperator(
        task_id='params',
        op_args=[batch_id()],
        python_callable=validate_params,
        on_execute_callback=Slack.notify_dag_start,
    )

    create = FranklinCreate(
        group_id='create',
        batch_id=batch_id(),
        skip='',
    )

    update = FranklinUpdate(
        group_id='update',
        batch_id=batch_id(),
        skip='',
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    params >> create >> update >> slack