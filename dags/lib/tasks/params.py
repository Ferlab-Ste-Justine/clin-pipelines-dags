from typing import List

from airflow.decorators import task
from airflow.operators.python import get_current_context


@task(task_id='get_batch_ids')
def get_batch_ids() -> List[str]:
    context = get_current_context()
    params = context["params"]
    ids = params['batch_ids'] if params ['batch_ids'] is not None else []
    # try to keep the somatic_normal imported last
    return sorted(set(ids), key=lambda x: (x.endswith("somatic_normal"), x))


@task(task_id='get_sequencing_ids')
def get_sequencing_ids() -> list:
    context = get_current_context()
    params = context["params"]
    return params['sequencing_ids'] if params['sequencing_ids'] is not None else []

@task(task_id='get_analysis_ids')
def get_analysis_ids() -> list:
    context = get_current_context()
    params = context["params"]
    return params['analysis_ids'] if params['analysis_ids'] is not None else []

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
