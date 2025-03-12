from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.config import Env, env
from lib.slack import Slack
from sensors.rolling import RollingAutoSensor

if env == Env.QA:
    with DAG(
            dag_id='etl_rolling_auto',
            start_date=datetime(2022, 1, 1),
            schedule_interval="0 6 * * *" if env == Env.QA else None, # 6am UTC / 2am Montreal
            catchup=False,
            params={

            },
            default_args={
                'on_failure_callback': Slack.notify_task_failure,
                'trigger_rule': TriggerRule.NONE_FAILED,
            },
            max_active_runs=1
    ) as dag:

        slack_start = EmptyOperator(
            task_id="slack_start",
            on_success_callback=Slack.notify_dag_start
        )

        sensor = RollingAutoSensor(
            task_id='sensor',
            mode='poke',
            soft_fail=True, 
            poke_interval=60, # 1 min
            timeout=60 * 60 * 16 # 16 hours, based on the schedule, DAG will run every day for 16h
        )

        slack_end = EmptyOperator(
            task_id="slack_end",
            on_success_callback=Slack.notify_dag_completion
        )
        
    slack_start >> sensor >> slack_end
