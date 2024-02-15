from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from lib.doc import qc as doc
from lib.groups.qc import qc
from lib.slack import Slack
from lib.tasks.params_validate import validate_release
from lib.utils_etl import release_id, spark_jar

with DAG(
    dag_id='etl_qc',
    doc_md=doc.etl_qc,
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'release_id': Param('', type='string'),
        'spark_jar': Param('', type=['null', 'string']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
    },
    max_active_tasks=4
) as dag:
    params_validate = validate_release(release_id())

    qc = qc(
        group_id='qc',
        release_id=release_id(),
        spark_jar=spark_jar(),
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    params_validate >> qc >> slack
