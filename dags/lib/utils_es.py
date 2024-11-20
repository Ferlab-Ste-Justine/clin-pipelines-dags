from lib.config import env, es_url
from lib.utils_etl import color


def format_es_url(index, _color=None, release_id=None, suffix=None):
    url = f'{es_url}/clin_{env}'
    if _color:
        url += _color
    url += f'_{index}'
    if release_id:
        url += f'_{release_id}'
    if suffix:
        url += suffix
    return url

