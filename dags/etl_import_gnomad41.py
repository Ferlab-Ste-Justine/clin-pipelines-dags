import logging
from datetime import datetime
from enum import Enum

import boto3

from botocore import UNSIGNED
from botocore.config import Config

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config
from lib.config import clin_datalake_bucket
from lib.slack import Slack
from lib.utils_import import stream_upload_to_s3, get_s3_file_version

class SequencingType(Enum):
    GENOMES = 'genomes'
    EXOMES = 'exomes'

LATEST_VERSION = '4.1'
GNOMAD_S3_BUCKET = 'gnomad-public-us-east-1'

def get_file_names(s3_client, bucket_name, prefix=""):
    file_names = []

    default_kwargs = {
        "Bucket": bucket_name,
        "Prefix": prefix
    }

    next_token = ""

    while next_token is not None:
        updated_kwargs = default_kwargs.copy()
        if next_token != "":
            updated_kwargs["ContinuationToken"] = next_token

        response = s3_client.list_objects_v2(**default_kwargs)
        contents = response.get("Contents")

        for result in contents:
            key = result.get("Key")
            if key[-1] != "/":
                file_names.append(key)

        next_token = response.get("NextContinuationToken")

    return file_names

with DAG(
        dag_id='etl_import_gnomad_v4_genomes_exomes',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        max_active_tasks=1,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        }
) as dag:
    logging.info(f'gnomAD genomes and exomes latest version: {LATEST_VERSION}')

    clin_s3 = S3Hook(config.s3_conn_id)
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    def _download_files(sequencing_type: SequencingType):
        seq_type = sequencing_type.value
        destination_prefix = f'raw/landing/gnomad_v4/{seq_type}'
        gnomad_prefix = f'release/{LATEST_VERSION}/vcf/{seq_type}'

        # Get imported version
        imported_ver = get_s3_file_version(clin_s3, clin_datalake_bucket, destination_prefix)
        imported_ver = 'v4.0'
        logging.info(f'Current gnomAD {seq_type} imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == LATEST_VERSION:
            logging.warning(f'Skipping import of gnomAD {seq_type}. Imported version {imported_ver} is up to date.')
            raise AirflowSkipException()

        # Download files
        logging.info(f'Importing gnomAD {seq_type} version: {LATEST_VERSION}')
        keys = get_file_names(s3_client, GNOMAD_S3_BUCKET, gnomad_prefix)

        for key in keys:
            generation_params = {'Bucket': GNOMAD_S3_BUCKET, 'Key': f'{gnomad_prefix}/{key}'}
            presigned_url = s3_client.generate_presigned_url("get_object", Params=generation_params)
            destination_key = f'{destination_prefix}/{key}'

            logging.info(f'Importing file {key}')

            stream_upload_to_s3(clin_s3, clin_datalake_bucket, destination_key, presigned_url)

        # Update version
        logging.info(f'Version {LATEST_VERSION} of gnomAD {seq_type} imported to S3.')
        clin_s3.load_string(LATEST_VERSION, f'{destination_prefix}.version', clin_datalake_bucket, replace=True)

    genome_files = PythonOperator(
        task_id='genome_files',
        op_args=[SequencingType.GENOMES],
        python_callable=_download_files
    )

    exome_files = PythonOperator(
        task_id='exome_files',
        op_args=[SequencingType.EXOMES],
        python_callable=_download_files
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    [genome_files, exome_files] >> slack