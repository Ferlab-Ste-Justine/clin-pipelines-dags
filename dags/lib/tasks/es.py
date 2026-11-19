import logging

import requests
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from lib.config import env, es_url
from lib.utils_es import color, format_es_url


def check_release_format(release: str):
    if not release.startswith('re_') or len(release) != 6:
        raise AirflowFailException(f'Invalid release format ({release}). Expected format: re_XXX where XXX are digits.')


def get_previous_release(release: str, n: int = 2):
    check_release_format(release)
    num = int(release[3:])
    previous_num = num - n
    if previous_num < 0:
        raise AirflowSkipException("Previous release number is less than 0.")
    previous_release = f"re_{previous_num:03d}"
    return previous_release


def get_live_indices(index_name: str, color: str) -> list:
    alias = f'clin_{env}{color}_{index_name}'
    response = requests.get(f'{es_url}/_alias/{alias}', verify=False)
    if response.status_code == 404:
        return []
    if not response.ok:
        raise AirflowFailException(f'Failed to read alias {alias}: {response.text}')
    return list(response.json())


def delete_release_indices(index_names: list, release: str, color: str):
    check_release_format(release)
    indices = {name: f'clin_{env}{color}_{name}_{release}' for name in index_names}

    # never delete what the alias still serves, checked for all before deleting any
    live = [index for name, index in indices.items() if index in get_live_indices(name, color)]
    if live:
        raise AirflowFailException(f'Refusing to delete {", ".join(live)}: still live behind its alias')

    for index in indices.values():
        logging.info(f'Delete index: {index}')
        response = requests.delete(f'{es_url}/{index}?ignore_unavailable=true', verify=False)
        logging.info(f'ES response:\n{response.text}')
        if not response.ok:
            raise AirflowFailException('Failed')


@task(task_id='delete_previous_release')
def delete_previous_release(index_name: str, release_id: str, color: str, skip=None):
    if skip:
        raise AirflowSkipException()

    delete_release_indices([index_name], get_previous_release(release_id), color)


@task(task_id='delete_release')
def delete_release(index_names: list, release_id: str, color: str, skip=None):
    if skip:
        raise AirflowSkipException()

    delete_release_indices(index_names, release_id, color)


@task(task_id='set_previous_release_replicas')
def set_previous_release_replicas(index_name: str, release_id: str, color: str, replicas: int, skip=None):
    if skip:
        raise AirflowSkipException()

    previous_release = get_previous_release(release_id, 1)
    index = f'clin_{env}{color}_{index_name}_{previous_release}'

    logging.info(f'Set number_of_replicas={replicas} for index: {index}')

    response = requests.put(f'{es_url}/{index}/_settings',
                            json={'index': {'number_of_replicas': replicas}}, verify=False)
    logging.info(f'ES response:\n{response.text}')

    if response.status_code == 404:
        raise AirflowSkipException(f'Index {index} does not exist')

    if not response.ok:
        raise AirflowFailException('Failed')

    return


@task(task_id='test_duplicated_by_url')
def test_duplicated_by_url(url, skip=None):
    if skip:
        raise AirflowSkipException()

    headers = {'Content-Type': 'application/json'}
    body = {
        "size": 0,
        "aggs": {
            "duplicated": {
                "terms": {
                    "field": 'hash',
                    "min_doc_count": 2,
                    "size": 1
                }
            }
        }
    }
    response = requests.post(url, headers=headers, json=body, verify=False)
    logging.info(f'ES response: {response.text}')
    buckets = response.json().get('aggregations', {}).get('duplicated', {}).get('buckets', [])
    if not response.ok or len(buckets) > 0:
        raise AirflowFailException('Failed')
    return


@task(task_id='es_test_disk_usage')
def test_disk_usage(skip=None):
    if skip:
        raise AirflowSkipException()

    response = requests.get(f'{es_url}/_cat/allocation?v&pretty', verify=False)
    logging.info(f'ES response:\n{response.text}')

    # max across nodes, rows without disk stats (UNASSIGNED) are ignored
    lines = response.text.strip().split('\n')
    percent_column = lines[0].split().index('disk.percent')
    usages = [float(row[percent_column]) for row in (line.split() for line in lines[1:])
              if len(row) > percent_column and row[percent_column].replace('.', '', 1).isdigit()]
    if not usages:
        raise AirflowFailException('Failed to parse ES disk usage')
    disk_usage = max(usages)

    logging.info(f'ES disk usage: {disk_usage}%')

    if disk_usage > 80:
        raise AirflowFailException(
            f'ES disk usage is too high: {disk_usage}% please delete some old releases')
    return


@task(task_id='get_release_id')
def get_release_id(release_id: str, color: str, index: str, increment: bool = True, skip: bool = False):
    if skip:
        raise AirflowSkipException()

    if release_id:
        logging.info(f'Using release id passed to DAG: {release_id}')
        return release_id

    logging.info(f'No release id passed to DAG. Fetching release id from ES for index {index}.')
    # Fetch current id from ES
    url = format_es_url(index, _color=color, suffix='?&pretty')
    response = requests.get(url, verify=False)
    logging.info(f'ES response:\n{response.text}')

    # Parse current id
    current_full_release_id = list(response.json())[0]  # clin_{env}_{index}_re_0xx
    current_release_id = current_full_release_id.split('_')[-1]  # 0xx
    logging.info(f'Current release id: re_{current_release_id}')

    if increment:
        # Increment current id by 1
        new_release_id = f're_{str(int(current_release_id) + 1).zfill(3)}'
        logging.info(f'New release id: {new_release_id}')
        return new_release_id
    else:
        return f're_{current_release_id}'
