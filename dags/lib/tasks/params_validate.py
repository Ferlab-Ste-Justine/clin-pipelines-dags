import logging
from typing import List

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from lib.config import Env, env
from lib.slack import Slack
from lib.utils_etl import get_current_color


def _validate_color(color: str) -> None:
    if env == Env.QA:
        if not color or color == '' or color not in ['green', 'blue']:
            raise AirflowFailException(
                f'DAG param "color" blue or green is required in {env} environment'
            )
    elif color and color != '':
        raise AirflowFailException(
            f'DAG param "color" is forbidden in {env} environment'
        )


def _validate_release_id(release_id: str) -> None:
    if release_id == '':
        raise AirflowFailException('DAG param "release_id" is required')


@task(task_id='params_validate', on_execute_callback=Slack.notify_dag_start)
def validate_release_color(release_id: str, color: str):
    _validate_release_id(release_id)
    _validate_color(color)


@task(task_id='params_validate', on_execute_callback=Slack.notify_dag_start)
def validate_batch_color(batch_id: str, color: str):
    if batch_id == '':
        raise AirflowFailException('DAG param "batch_id" is required')
    _validate_color(color)


@task(task_id='params_validate', on_execute_callback=Slack.notify_dag_start)
def validate_release(release_id: str):
    _validate_release_id(release_id)


@task(task_id='params_validate', on_execute_callback=Slack.notify_dag_start)
def validate_color(color: str, **context):
    env_color = color
    # If the DAG is triggered via scheduler or API request, we need to get the current color from Elasticsearch
    if not env_color and env == Env.QA:
        env_color = get_current_color()
        logging.info(f'No color found using current ES: {env_color}')
   
    _validate_color(env_color)
    return env_color

@task(task_id='params_validate', on_execute_callback=Slack.notify_dag_start)
def validate_batch_analysis_ids_color(batch_id: str, analysis_ids: List[str], color: str):
    batch_ids = [batch_id] if batch_id and batch_id != "" else []
    validate_batch_ids_analysis_ids_color(batch_ids, analysis_ids, color)

@task(task_id='params_validate', on_execute_callback=Slack.notify_dag_start)
def validate_batch_ids_analysis_ids_color(batch_ids: List[str], analysis_ids: List[str], color: str):
    if not batch_ids and not analysis_ids:
        raise AirflowFailException(
            'Either "batch_ids" or "analysis_ids" DAG param is required'
        )
    if batch_ids and analysis_ids:
        raise AirflowFailException(
            'DAG params "batch_ids" and "analysis_ids" cannot be used together'
        )
    _validate_color(color)
