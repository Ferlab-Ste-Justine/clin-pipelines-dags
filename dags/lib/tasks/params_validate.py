from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import DagRun
from lib.config import Env, env
from lib.slack import Slack


@task(task_id='params_validate', on_execute_callback=Slack.notify_dag_start)
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


@task(task_id='params_validate', on_execute_callback=Slack.notify_dag_start)
def validate_batch_sequencing_ids_color(batch_id: str, sequencing_ids: str, color: str):
    if batch_id == '' and sequencing_ids == '':
        raise AirflowFailException('DAG param "batch_id" or "sequencing_ids" is required')
    if env == Env.QA:
        if not color or color == '':
            raise AirflowFailException(
                f'DAG param "color" is required in {env} environment'
            )
    elif color and color != '':
        raise AirflowFailException(
            f'DAG param "color" is forbidden in {env} environment'
        )


@task(task_id='params_validate', on_execute_callback=Slack.notify_dag_start)
def validate_release(release_id: str):
    if release_id == '':
        raise AirflowFailException('DAG param "release_id" is required')


@task(task_id='params_validate', on_execute_callback=Slack.notify_dag_start)
def validate_color(color: str):
    if env == Env.QA:
        if not color or color == '' or not (color == 'green' or color == 'blue'):
            raise AirflowFailException(
                f'DAG param "color" blue or green is required in {env} environment'
            )
    elif color and color != '':
        raise AirflowFailException(
            f'DAG param "color" is forbidden in {env} environment'
        )

@task(task_id='get_sequencing_ids')
def get_sequencing_ids(ti=None) -> list:
    dag_run: DagRun = ti.dag_run
    return dag_run.conf['sequencing_ids'] if dag_run.conf['sequencing_ids'] is not None else []