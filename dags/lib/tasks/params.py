from typing import List

from airflow.decorators import task
from airflow.models import DagRun


@task(task_id='get_batch_ids')
def get_batch_ids(ti) -> List[str]:
    dag_run: DagRun = ti.dag_run
    ids = dag_run.conf['batch_ids'] if dag_run.conf['batch_ids'] is not None else []
    # try to keep the somatic_normal imported last
    return sorted(set(ids), key=lambda x: (x.endswith("somatic_normal"), x))


@task(task_id='get_sequencing_ids')
def get_sequencing_ids(ti=None) -> list:
    dag_run: DagRun = ti.dag_run
    return dag_run.conf['sequencing_ids'] if dag_run.conf['sequencing_ids'] is not None else []

@task(task_id='get_analysis_ids')
def get_analysis_ids(ti=None) -> list:
    dag_run: DagRun = ti.dag_run
    return dag_run.conf['analysis_ids'] if dag_run.conf['analysis_ids'] is not None else []

@task(task_id='prepare_expand_batch_ids')
def prepare_expand_batch_ids(batch_ids: List[str], skip: bool):
    '''
    this is a workaround solution to the (what we think to be) a bug of Airflow
    that won't create but automatically remove a task when using expand(batch_id=[])
    This shall only apply in the situation we are skip.
    '''
    if skip and len(batch_ids) == 0:
        return ['this_is_a_workaround_batch_id']
    return batch_ids
