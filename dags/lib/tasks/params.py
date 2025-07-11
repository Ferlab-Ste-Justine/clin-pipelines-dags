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
