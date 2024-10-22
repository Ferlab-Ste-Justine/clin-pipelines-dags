import logging
from datetime import datetime
from enum import Enum

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


with DAG(
        dag_id='etl_import_gnomad_v4_genomes_exomes',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        max_active_tasks=1,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        }
) as dag:
    # Get latest version
    latest_ver = '4.0'
    logging.info(f'gnomAD genomes and exomes latest version: {latest_ver}')

    clin_s3 = S3Hook(config.s3_conn_id)
    gnomad_s3 = S3Hook(config.s3_gnomad)
    logging.info(gnomad_s3.get_connection("s3_gnomad").extra)
    gnomad_s3_bucket = 'gnomad-public-us-east-1'

    def _download_files(sequencing_type: SequencingType):
        seq_type = sequencing_type.value
        destination_prefix = f'raw/landing/gnomad_v4/{seq_type}'
        gnomad_prefix = f'release/{latest_ver}/vcf/{seq_type}'

        # Get imported version
        imported_ver = get_s3_file_version(clin_s3, clin_datalake_bucket, destination_prefix)
        logging.info(f'Current gnomAD {seq_type} imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            logging.warning(f'Skipping import of gnomAD {seq_type}. Imported version {imported_ver} is up to date.')
            raise AirflowSkipException()

        # Download files
        logging.info(f'Importing gnomAD {seq_type} version: {latest_ver}')
        keys = gnomad_s3.list_keys(gnomad_s3_bucket, f'{gnomad_prefix}/')
        for key in keys:
            presigned_url = gnomad_s3.generate_presigned_url(client_method="get_object",
                                                             params={'Bucket': gnomad_s3_bucket,
                                                                     'Key': f'{gnomad_prefix}/{key}'})
            destination_key = f'{destination_prefix}/{key}'
            logging.info(f'Importing file {key}')
            stream_upload_to_s3(clin_s3, clin_datalake_bucket, destination_key, presigned_url)

        # Update version
        logging.info(f'Version {latest_ver} of gnomAD {seq_type} imported to S3.')
        clin_s3.load_string(latest_ver, f'{destination_prefix}.version', clin_datalake_bucket, replace=True)


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
