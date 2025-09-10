import json
import logging
from typing import Any

import requests
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
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
        s3.load_file_obj(response.raw, s3_key, s3_bucket, replace)


def stream_upload_or_resume_to_s3(s3: S3Hook, s3_bucket: str, s3_key: str, url: str, partSizeMb: int = 200, md5: str = None) -> None:
    try:
        s3_client = s3.get_conn()
        parts = []
        part_number = 1
        file_size=0
        uploaded_bytes=0
        headers = {}

        # Check if an UploadId already exists to resume download
        upload_id = get_s3_multipart_upload_id(s3, s3_bucket, s3_key)
        if upload_id:
            # Get the list of already uploaded parts
            dict = s3_client.list_parts(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id)
            retrieved_parts = dict.get('Parts', [])
            parts = [{'PartNumber': part['PartNumber'], 'ETag': part['ETag']} for part in retrieved_parts]
            part_number = len(parts) + 1
            uploaded_bytes = sum(part['Size'] for part in retrieved_parts)
            headers['Range'] = f'bytes={uploaded_bytes}-'
        else:
            mpu_response = s3_client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)
            upload_id = mpu_response['UploadId']

        with requests.get(url, stream=True, headers=headers) as r:
            # If resuming, check response status
            if len(parts) > 0 and r.status_code != 206:
                logging.info("File cannot be resumed, starting from the beginning")
                uploaded_bytes = 0
                parts = []
                part_number = 1

            file_size = int(r.headers['Content-Length']) + uploaded_bytes
            r.raise_for_status()

            if uploaded_bytes == 0:
                logging.info(f"Start upload of '{url}' ({bytes_to_human_readable(file_size)}), to {s3_key}")
            else:
                logging.info(f"Resuming upload of '{url}' ({bytes_to_human_readable(file_size)}), to {s3_key} ({bytes_to_human_readable(uploaded_bytes)} already downloaded)")

            for chunk in r.iter_content(chunk_size=partSizeMb * 1024 * 1024):
                if chunk:  # filter out keep-alive new chunks
                    part_response = s3_client.upload_part(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )
                    parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
                    part_number += 1
                    uploaded_bytes += len(chunk)
                    percentage = (uploaded_bytes / file_size) * 100
                    logging.info(f"Uploaded {bytes_to_human_readable(uploaded_bytes)} of {bytes_to_human_readable(file_size)} ({percentage:.2f}%)")

        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=s3_bucket,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        # Save md5 checksum
        if(md5):
            s3.load_string(md5, f'{s3_key}.md5', s3_bucket, replace=True)

        logging.info(f"Multipart upload of {s3_key} from {url} completed successfully")

    except Exception as e:
        logging.error(f"Error during multipart upload: {e}")
        raise e


def get_s3_multipart_upload_id(s3: S3Hook, s3_bucket: str, s3_key: str) -> str | None:
    s3_client = s3.get_conn()
    response = s3_client.list_multipart_uploads(Bucket=s3_bucket, Prefix=s3_key)
    uploads = response.get('Uploads', [])
    upload_id = [upload['UploadId'] for upload in uploads]
    return upload_id[0] if upload_id else None


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

def bytes_to_human_readable(byteSize: int) -> str:
    mbBytes = byteSize / (1024 * 1024)
    if(mbBytes > 1000):
        gbBytes = mbBytes / 1024
        return f"{gbBytes:.2f} GB"
    return f"{mbBytes:.2f} MB"