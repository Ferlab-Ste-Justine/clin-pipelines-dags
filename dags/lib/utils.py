import base64
import hashlib
import json
from typing import Any, List
import re

import requests


# Airflow's standard skip mechanism (exceptions) does not work in virtualenv
# tasks, so we use this custom exit code to signal a skipped task.
SKIP_EXIT_CODE = 99


def join(string: str, parts: List[str]) -> str:
    return string.join(filter(None, parts))


def http_get(url: str, headers: Any = None) -> requests.Response:
    with requests.get(url, headers=headers) as response:
        response.raise_for_status()
        return response


def http_get_file(url: str, path: str, headers: Any = None, chunk_size: int = 8192, md5: str = None, stream=True, **kwargs) -> None:
    with requests.get(url, headers=headers, stream=stream, **kwargs) as response:
        response.raise_for_status()
        with open(path, 'wb') as file:
            for chunk in response.iter_content(chunk_size):
                file.write(chunk)

        if md5:
            download_md5_hash = file_md5(path)
            if download_md5_hash != md5:
                raise Exception(f'MD5 checksum verification failed for: {path}, expected {md5} but got {download_md5_hash}')


def http_post(url: str, json: Any = None) -> requests.Response:
    with requests.post(url, json=json) as response:
        response.raise_for_status()
        return response


def file_md5(path: str, chunk_size: int = 8192) -> str:
    md5 = hashlib.md5()
    with open(path, 'rb') as file:
        for chunk in iter(lambda: file.read(chunk_size), b''):
            md5.update(chunk)
        return md5.hexdigest()
    

def get_md5_from_url(url: str) -> dict:
    md5_text = http_get(url).text
    md5_hash = re.search('^([0-9a-f]+)', md5_text).group(1)
    return { 'hash': md5_hash, 'text': md5_text }


def urlsafe_hash(obj: Any, length: int) -> str:
    utf8_str = json.dumps(obj, default=str).encode('utf-8')  # Ensure consistent encoding
    hash_obj = hashlib.sha256(utf8_str).digest()
    base64_str = base64.urlsafe_b64encode(hash_obj).decode('utf-8').rstrip('=')  # Encoding to base64 allows for more compact representation (more bits per character)
    return base64_str[:length]
