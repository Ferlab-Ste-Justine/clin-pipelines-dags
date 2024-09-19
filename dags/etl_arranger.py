from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.config import Env, K8sContext, env, es_url
from lib.operators.curl import CurlOperator
from lib.slack import Slack
from lib.tasks import arranger
from lib.tasks.params_validate import validate_release_color
from lib.utils_etl import color, release_id

with DAG(
        dag_id='etl_arranger',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={

        },
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
            'trigger_rule': TriggerRule.NONE_FAILED,
        },
) as dag:

    slack_start = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_start
    )

    arranger_remove_project_task = arranger.remove_project()
    arranger_restart_task = arranger.restart(on_success_callback=Slack.notify_dag_completion)
    

    slack_start >> arranger_remove_project_task >> arranger_restart_task