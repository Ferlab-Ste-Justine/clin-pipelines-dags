import logging

import requests
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from lib.config import env, es_url


def _is_force_merge_running(full_index: str, timeout: int = 10) -> bool:
    url = f'{es_url}/_tasks?actions=indices:admin/forcemerge&detailed=true'
    try:
        response = requests.get(url, verify=False, timeout=timeout)
        if not response.ok:
            logging.warning(f'Could not query running force merges: {response.text}')
            return False
        data = response.json()
        for node in data.get('nodes', {}).values():
            for task_info in node.get('tasks', {}).values():
                description = task_info.get('description', '')
                if full_index in description:
                    logging.info(f'Found running force merge for [{full_index}]: {description}')
                    return True
        return False
    except requests.exceptions.RequestException as e:
        logging.warning(f'Force-merge task lookup failed: {e}')
        return False


def _check_disk_available(min_free_percent: float, timeout: int = 10):
    url = f'{es_url}/_cluster/stats'
    response = requests.get(url, verify=False, timeout=timeout)
    if not response.ok:
        raise AirflowFailException(f'Cluster stats query failed: {response.text}')

    fs = response.json().get('nodes', {}).get('fs', {})
    total = fs.get('total_in_bytes', 0)
    available = fs.get('available_in_bytes', 0)
    if total == 0:
        logging.warning('Cluster fs stats unavailable, skipping disk check')
        return

    free_percent = (available / total) * 100
    logging.info(
        f'Cluster disk: {available / 1e9:.1f}GB free / {total / 1e9:.1f}GB total ({free_percent:.1f}%)'
    )
    if free_percent < min_free_percent:
        raise AirflowFailException(
            f'Cluster disk free {free_percent:.1f}% below threshold {min_free_percent}%. '
            f'Aborting force merge to prevent disk exhaustion.'
        )


@task(task_id='wait_for_ready')
def wait_for_ready(index_name: str, release_id: str, color: str, skip=None, es_timeout_minutes: int = 30):
    """Wait for index to reach green health (all replicas STARTED)."""
    if skip:
        raise AirflowSkipException()

    full_index = f'clin_{env}{color}_{index_name}_{release_id}'
    es_timeout = f'{es_timeout_minutes}m'
    requests_timeout = es_timeout_minutes * 60 + 10  # ES timeout + 10s buffer
    url = f'{es_url}/_cluster/health/{full_index}?wait_for_status=green&wait_for_no_relocating_shards=true&timeout={es_timeout}'

    logging.info(f'Waiting for ready status on index [{full_index}] (timeout={es_timeout})')
    response = requests.get(url, verify=False, timeout=requests_timeout)
    logging.info(f'ES response: {response.text}')

    if not response.ok:
        raise AirflowFailException(f'Cluster health check failed for {full_index}: {response.text}')

    result = response.json()
    if result.get('timed_out', False) or result.get('status') != 'green':
        raise AirflowFailException(
            f'Index {full_index} did not reach green within {es_timeout} '
            f'(status={result.get("status")}, timed_out={result.get("timed_out")})'
        )

    logging.info(f'Index [{full_index}] is green')


@task(task_id='force_merge')
def force_merge(index_name: str, release_id: str, color: str, skip=None,
                dryrun: str = '',
                max_num_segments: int = 5, timeout: int = 10,
                min_disk_free_percent: float = 30.0):
    """Trigger force merge. Best-effort: logs warning on timeout (merge continues on ES).

    Prechecks: skip if a force merge is already running on this index (idempotency),
    abort if cluster disk free falls below threshold (force merge transiently 2-3x's index size).
    When dryrun == 'yes', everything runs (prechecks, logging) except the actual force-merge POST.
    """
    if skip:
        raise AirflowSkipException()

    full_index = f'clin_{env}{color}_{index_name}_{release_id}'
    is_dryrun = dryrun == 'yes'

    if _is_force_merge_running(full_index):
        raise AirflowSkipException(f'Force merge already running on [{full_index}]')

    _check_disk_available(min_free_percent=min_disk_free_percent)

    url = f'{es_url}/{full_index}/_forcemerge?max_num_segments={max_num_segments}'

    if is_dryrun:
        logging.info(f'[DRY RUN] Would POST {url} — skipping actual force merge on [{full_index}]')
        return

    logging.info(f'Triggering force merge on [{full_index}] (max_num_segments={max_num_segments})')
    try:
        response = requests.post(url, verify=False, timeout=timeout)
        logging.info(f'ES response: {response.text}')
        if not response.ok:
            logging.warning(f'Force merge returned error for {full_index}: {response.text} (merge may still be running)')
    except requests.exceptions.Timeout:
        logging.warning(f'Force merge timed out for {full_index} — merge continues in background on ES')
