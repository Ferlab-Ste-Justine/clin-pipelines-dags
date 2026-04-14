import logging

import requests
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from lib.config import env, es_url


@task(task_id='wait_for_ready')
def wait_for_ready(index_name: str, release_id: str, color: str, skip=None, timeout: str = '30m'):
    """Wait for index to reach green health (all replicas STARTED)."""
    if skip:
        raise AirflowSkipException()

    full_index = f'clin_{env}{color}_{index_name}_{release_id}'
    url = f'{es_url}/_cluster/health/{full_index}?wait_for_status=green&wait_for_no_relocating_shards=true&timeout={timeout}'

    logging.info(f'Waiting for ready status on index [{full_index}] (timeout={timeout})')
    response = requests.get(url, verify=False)
    logging.info(f'ES response: {response.text}')

    if not response.ok:
        raise AirflowFailException(f'Cluster health check failed for {full_index}: {response.text}')

    result = response.json()
    if result.get('timed_out', False) or result.get('status') != 'green':
        raise AirflowFailException(
            f'Index {full_index} did not reach green within {timeout} '
            f'(status={result.get("status")}, timed_out={result.get("timed_out")})'
        )

    logging.info(f'Index [{full_index}] is green')


@task(task_id='force_merge')
def force_merge(index_name: str, release_id: str, color: str, skip=None, max_num_segments: int = 1, timeout: int = 3600):
    """Trigger force merge. Best-effort: logs warning on timeout (merge continues on ES)."""
    if skip:
        raise AirflowSkipException()

    full_index = f'clin_{env}{color}_{index_name}_{release_id}'
    url = f'{es_url}/{full_index}/_forcemerge?max_num_segments={max_num_segments}'

    logging.info(f'Triggering force merge on [{full_index}] (max_num_segments={max_num_segments})')
    try:
        response = requests.post(url, verify=False, timeout=timeout)
        logging.info(f'ES response: {response.text}')
        if not response.ok:
            logging.warning(f'Force merge returned error for {full_index}: {response.text} (merge may still be running)')
    except requests.exceptions.Timeout:
        logging.warning(f'Force merge timed out for {full_index} — merge continues in background on ES')
