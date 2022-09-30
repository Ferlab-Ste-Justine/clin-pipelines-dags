from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib.config import env, Env, K8sContext
from lib.operators.pipeline import PipelineOperator
from lib.slack import Slack


with DAG(
    dag_id='etl_notify',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'batch_id':  Param('', type='string'),
        'color': Param('', enum=['', 'blue', 'green']),
    },
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def batch_id() -> str:
        return '{{ params.batch_id }}'

    def color(prefix: str = '') -> str:
        return '{% if params.color|length %}' + prefix + '{{ params.color }}{% endif %}'

    def _params_validate(batch_id, color):
        if batch_id == '':
            raise AirflowFailException('DAG param "batch_id" is required')
        if env == Env.QA:
            if color == '':
                raise AirflowFailException(
                    f'DAG param "color" is required in {env} environment'
                )
        elif color != '':
            raise AirflowFailException(
                f'DAG param "color" is forbidden in {env} environment'
            )

    params_validate = PythonOperator(
        task_id='params_validate',
        op_args=[batch_id(), color()],
        python_callable=_params_validate,
        on_execute_callback=Slack.notify_dag_start,
    )

    notify = PipelineOperator(
        task_id='notify',
        name='etl-notify',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        arguments=[
            'bio.ferlab.clin.etl.LDMNotifier', batch_id(),
        ],
        on_success_callback=Slack.notify_dag_complete,
    )

    params_validate >> notify
