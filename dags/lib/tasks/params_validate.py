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
def validate_batch_analysis_ids_color(batch_id: str, analysis_ids: str, color: str):
    if batch_id == '' and analysis_ids == '':
        raise AirflowFailException('DAG param "batch_id" or "analysis_ids" is required')
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

@task(task_id='get_batch_ids')
def get_batch_ids(ti=None) -> list:
    dag_run: DagRun = ti.dag_run
    ids = dag_run.conf['batch_ids'] if dag_run.conf['batch_ids'] is not None else []
    # try to keep the somatic_normal imported last
    return sorted(set(ids), key=lambda x: (x.endswith("somatic_normal"), x)) 

@task(task_id='get_analysis_ids')
def get_analysis_ids(ti=None) -> list:
    dag_run: DagRun = ti.dag_run
    return dag_run.conf['analysis_ids'] if dag_run.conf['analysis_ids'] is not None else []