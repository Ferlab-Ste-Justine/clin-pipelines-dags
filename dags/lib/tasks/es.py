import logging

import requests
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from lib.config import env, es_url
from lib.utils_es import color, format_es_url


def get_previous_release(release: str, n: int = 2):
    if not release.startswith('re_') or len(release) != 6:
        raise AirflowFailException("Invalid release format. Expected format: re_XXX where XXX are digits.")
    num = int(release[3:])
    previous_num = num - n
    if previous_num < 0:
        raise AirflowSkipException("Previous release number is less than 0.")
    previous_release = f"re_{previous_num:03d}"
    return previous_release


@task(task_id='delete_previous_release')
def delete_previous_release(index_name: str, release_id: str, color: str, skip=None):
    if skip:
        raise AirflowSkipException()

    previous_release = get_previous_release(release_id)

    logging.info(f'Delete previous release for index: {index_name} {previous_release}')

    response = requests.delete(f'{es_url}/clin_{env}{color}_{index_name}_{previous_release}?ignore_unavailable=true', verify=False)
    logging.info(f'ES response:\n{response.text}')

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

    first_node_usage = response.text.split('\n')[1]
    first_node_disk_usage = first_node_usage.split()[5]

    logging.info(f'ES disk usage: {first_node_disk_usage}%')

    if float(first_node_disk_usage) > 75:
        raise AirflowFailException(
            f'ES disk usage is too high: {first_node_disk_usage}% please delete some old releases')
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
