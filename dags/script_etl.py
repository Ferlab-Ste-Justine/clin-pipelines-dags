from datetime import datetime
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from lib.config import EtlConfig, K8sContext
from lib.operators.spark_etl import SparkOperator
from lib.slack import Slack
from lib.utils_etl import spark_jar, build_etl_job_arguments

logger = logging.getLogger(__name__)

SCRIPT_MAIN_CLASS = 'bio.ferlab.clin.etl.script.RunScript'

with DAG(
    dag_id='script_etl',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    schedule_interval=None,
    description="Run an ad-hoc Spark script.",
    params={
        'spark_class': Param(SCRIPT_MAIN_CLASS, type='string'),
        'spark_jar': Param('', type=['null', 'string']),
        'spark_config': Param(EtlConfig.LARGE.value, type='string', enum=[e.value for e in EtlConfig]),
        'entrypoint': Param('', type=['null', 'string']),
        'extra_args': Param([], type=['null', 'array']),
    },
    render_template_as_native_obj=True,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def spark_class() -> str:
        return '{{ params.spark_class }}'

    def spark_config() -> str:
        return '{{ params.spark_config }}'

    def entrypoint() -> str:
        return '{{ params.entrypoint }}'

    def extra_args() -> str:
        return '{{ params.extra_args or [] }}'

    @task(task_id="get_args")
    def get_args(extra_args, entrypoint):
        arguments = build_etl_job_arguments(
            app_name='script_etl',
            entrypoint=entrypoint
        )
        if extra_args:
            arguments += extra_args
        return arguments

    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )

    get_args_task = get_args(extra_args=extra_args(), entrypoint=entrypoint())

    script_etl_task = SparkOperator(
        task_id='script_etl',
        name='script_etl',
        k8s_context=K8sContext.ETL,
        spark_class=spark_class(),
        spark_config=spark_config(),
        spark_jar=spark_jar(),
        arguments=get_args_task,
        on_success_callback=Slack.notify_dag_completion,
    )

    start >> get_args_task >> script_etl_task
