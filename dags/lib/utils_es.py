from lib.config import es_url, env
from lib.utils_etl import color


def format_es_url(index, _color=None, release_id=None):
    url = f'{es_url}/clin_{env}'
    if _color:
        url += color("_")
    url += f'_{index}_centric'
    if release_id:
        url += f'_{release_id}'
    url += '/_search'
    return url
