from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from lib.config import Env, K8sContext, env
from lib.operators.pipeline import PipelineOperator
from lib.slack import Slack
from lib.utils_etl import color

with DAG(
    dag_id='script_pipeline',
    start_date=datetime(2022, 1, 1),
    schedule=None,
    params={
        'script': Param('', type='string'),
        'args': Param('', type=['null', 'string']),
        'color': Param('', type=['null', 'string']),
        'bucket': Param('', enum=['', f'cqgc-{env}-app-files-import', f'cqgc-{env}-app-datalake', f'cqgc-{env}-app-download',]),
    },
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def script() -> str:
        return '{{ params.script }}'

    def args() -> str:
        return '{{ params.args or "" }}'

    def bucket() -> str:
        return '{{ params.bucket }}'

    def _params_validate(script, color):
        if script == '':
            raise AirflowFailException(
                'DAG param "script" is required'
            )
        if env == Env.QA:
            if not color or color == '':
                raise AirflowFailException(
                    f'DAG param "color" is required in {env} environment'
                )
        elif color and color != '':
            raise AirflowFailException(
                f'DAG param "color" is forbidden in {env} environment'
            )

    params_validate = PythonOperator(
        task_id='params_validate',
        op_args=[script(), color()],
        python_callable=_params_validate,
        on_execute_callback=Slack.notify_dag_start,
    )

    script = PipelineOperator(
        task_id='script_pipeline',
        name='script_pipeline',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        on_success_callback=Slack.notify_dag_completion,
        aws_bucket=bucket(),
        arguments=[
            'bio.ferlab.clin.etl.Scripts', script(), args()
        ],
    )


    params_validate >> script
