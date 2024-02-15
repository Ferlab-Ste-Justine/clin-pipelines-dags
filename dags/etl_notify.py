from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

from lib.config import env, Env, K8sContext
from lib.operators.pipeline import PipelineOperator
from lib.slack import Slack
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
    def _params_validate(batch_id, color):
        if batch_id == '':
            raise AirflowFailException('DAG param "batch_id" is required')
        if env == Env.QA:
            if not color or color == '':
                raise AirflowFailException(
                    f'DAG param "color" is required in {env} environment'
                )
        elif color and color != '':
            raise AirflowFailException(
                f'DAG param "color" is forbidden in {env} environment'
            )


    params_validate = validate_batch_color(batch_id(), color())

    notify = PipelineOperator(
        task_id='notify',
        name='etl-notify',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        arguments=[
            'bio.ferlab.clin.etl.LDMNotifier', batch_id(),
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    params_validate >> notify
