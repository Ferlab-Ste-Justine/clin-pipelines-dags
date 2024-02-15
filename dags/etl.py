from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from lib.slack import Slack
from lib.tasks import batch_type
from lib.tasks.params_validate import validate_release_color
from lib.utils_etl import (batch_id, release_id, color, get_dag_config)

with DAG(
    dag_id='etl',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'batch_id': Param('', type=['null', 'string']),
        'release_id': Param('', type='string'),
        'color': Param('', type=['null', 'string']),
        'import': Param('yes', enum=['yes', 'no']),
        'notify': Param('no', enum=['yes', 'no']),
        'spark_jar': Param('', type=['null', 'string']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
    max_active_tasks=4,
    render_template_as_native_obj=True
) as dag:
    params_validate_task = validate_release_color.override(on_execute_callback=Slack.notify_dag_start)(
        release_id=release_id(),
        color=color()
    )

    detect_batch_type_task = batch_type.detect(batch_id())


    @task.branch(task_id='call_dag')
    def call_dag(batch_type: str):
        batch_type_dag_map = {
            'germline': 'etl_germline',
            'somatic_tumor_only': 'etl_somatic_tumor_only',
            'somatic_tumor_normal': 'etl_somatic_tumor_normal'
        }
        return batch_type_dag_map[batch_type]


    call_dag_task = call_dag(detect_batch_type_task)

    etl_germline_dag = TriggerDagRunOperator(
        task_id='etl_germline',
        trigger_dag_id='etl_germline',
        conf=get_dag_config(),
        wait_for_completion=True
    )

    etl_somatic_tumor_only_dag = TriggerDagRunOperator(
        task_id='etl_somatic_tumor_only',
        trigger_dag_id='etl_somatic_tumor_only',
        conf=get_dag_config(),
        wait_for_completion=True
    )

    etl_somatic_tumor_normal_dag = TriggerDagRunOperator(
        task_id='etl_somatic_tumor_normal',
        trigger_dag_id='etl_somatic_tumor_normal',
        conf=get_dag_config(),
        wait_for_completion=True
    )

    params_validate_task >> detect_batch_type_task >> call_dag_task >> [etl_germline_dag, etl_somatic_tumor_only_dag,
                                                                        etl_somatic_tumor_normal_dag]
