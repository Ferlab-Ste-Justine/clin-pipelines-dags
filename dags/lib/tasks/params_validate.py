from airflow.decorators import task
from airflow.exceptions import AirflowFailException

from lib.config import Env, env


@task(task_id='params_validate')
def validate_release_color(release_id: str, color: str):
    if release_id == '':
        raise AirflowFailException('DAG param "release_id" is required')
    if env == Env.QA:
        if not color or color == '':
            raise AirflowFailException(
                f'DAG param "color" is required in {env} environment'
            )
    elif color and color != '':
        raise AirflowFailException(
            f'DAG param "color" is forbidden in {env} environment'
        )


@task(task_id='params_validate')
def validate_batch_color(batch_id: str, color: str):
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


@task(task_id='params_validate')
def validate_release(release_id: str):
    if release_id == '':
        raise AirflowFailException('DAG param "release_id" is required')


@task(task_id='params_validate')
def validate_color(color: str):
    if env == Env.QA:
        if not color or color == '':
            raise AirflowFailException(
                f'DAG param "color" is required in {env} environment'
            )
    elif color and color != '':
        raise AirflowFailException(
            f'DAG param "color" is forbidden in {env} environment'
        )
