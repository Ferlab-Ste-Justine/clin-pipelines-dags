import json
from typing import Any

import requests
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib.utils import file_md5, http_get_file


def get_s3_file_md5(s3: S3Hook, s3_bucket: str, s3_key: str) -> str:
    s3_md5 = None
    s3_md5_file = f'{s3_key}.md5'
    if s3.check_for_key(s3_md5_file, s3_bucket):
        s3_md5 = s3.read_key(s3_md5_file, s3_bucket)
    return s3_md5


def get_s3_file_version(s3: S3Hook, s3_bucket: str, s3_key: str) -> str:
    s3_version = None
    s3_version_file = f'{s3_key}.version'
    if s3.check_for_key(s3_version_file, s3_bucket):
        s3_version = s3.read_key(s3_version_file, s3_bucket)
    return s3_version


def download_and_check_md5(url: str, file: str, expected_md5: str) -> None:
    http_get_file(f'{url}/{file}', file)
    md5 = file_md5(file)
    if expected_md5 is not None and md5 != expected_md5:
        raise AirflowFailException('MD5 checksum verification failed')
    return md5


def stream_upload_to_s3(s3: S3Hook, s3_bucket: str, s3_key: str, url: str, headers: Any = None, replace: bool = False, **kwargs) -> None:
    with requests.get(url, headers=headers, stream=True, **kwargs) as response:
        response.raw.chunked = True
        response.raise_for_status()
        with response as part:
            s3.load_file_obj(part.raw, s3_key, s3_bucket, replace)


def load_to_s3_with_md5(s3: S3Hook, s3_bucket: str, s3_key: str, file: str, file_md5: str) -> None:
    s3.load_file(file, s3_key, s3_bucket, replace=True)
    s3.load_string(file_md5, f'{s3_key}.md5', s3_bucket, replace=True)


def load_to_s3_with_version(s3: S3Hook, s3_bucket: str, s3_key: str, file: str, file_version: str) -> None:
    s3.load_file(file, s3_key, s3_bucket, replace=True)
    s3.load_string(file_version, f'{s3_key}.version', s3_bucket, replace=True)


def get_s3_storage_options(s3_conn_id: str) -> dict:
    conn = BaseHook.get_connection(s3_conn_id)
    host = json.loads(conn.get_extra()).get("host")
    storage_options = {
        "AWS_ACCESS_KEY_ID": conn.login,
        "AWS_SECRET_ACCESS_KEY": conn.get_password(),
        "AWS_ENDPOINT_URL": host,
        "AWS_ALLOW_HTTP": "true"  # For testing with local Minio
    }

    return storage_options
