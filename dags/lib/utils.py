import base64
import hashlib
import json
from typing import Any, List

import requests


def join(string: str, parts: List[str]) -> str:
    return string.join(filter(None, parts))


def http_get(url: str, headers: Any = None) -> requests.Response:
    with requests.get(url, headers=headers) as response:
        response.raise_for_status()
        return response


def http_get_file(url: str, path: str, headers: Any = None, chunk_size: int = 8192, **kwargs) -> None:
    with requests.get(url, headers=headers, stream=True, **kwargs) as response:
        response.raise_for_status()
        with open(path, 'wb') as file:
            for chunk in response.iter_content(chunk_size):
                file.write(chunk)


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


def urlsafe_hash(obj: Any, length: int) -> str:
    utf8_str = json.dumps(obj, default=str).encode('utf-8')  # Ensure consistent encoding
    hash_obj = hashlib.sha256(utf8_str).digest()
    base64_str = base64.urlsafe_b64encode(hash_obj).decode('utf-8').rstrip('=')  # Encoding to base64 allows for more compact representation (more bits per character)
    return base64_str[:length]
