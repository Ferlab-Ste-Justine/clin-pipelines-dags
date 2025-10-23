from typing import List

from airflow.decorators import task
from airflow.operators.python import get_current_context

# too many issues while getting the param from that dict, can be None, can be empty ...
# I'm writting that cause I have batchs to import and no time to investigate and we need the default value
def get_param(params: dict, paramName: str, defaultValue): 
    value = defaultValue
    if isinstance(params, dict):
        fromParam = params.get(paramName, defaultValue)
        if fromParam and len(fromParam) > 0:
            value = fromParam
    return value


@task(task_id='get_batch_ids')
def get_batch_ids() -> List[str]:
    context = get_current_context()
    params = context["params"]
    ids = get_param(params, 'batch_ids', [])
    # try to keep the somatic_normal imported last
    return sorted(set(ids), key=lambda x: (x.endswith("somatic_normal"), x)) if ids and len(ids) > 0 else []


@task(task_id='get_sequencing_ids')
def get_sequencing_ids() -> list:
    context = get_current_context()
    params = context["params"]
    return get_param(params, 'sequencing_ids', [])

@task(task_id='get_analysis_ids')
def get_analysis_ids() -> list:
    context = get_current_context()
    params = context["params"]
    return get_param(params, 'analysis_ids', [])

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
