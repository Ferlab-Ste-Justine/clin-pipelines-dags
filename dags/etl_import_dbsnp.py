import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib import config
from lib.config import K8sContext, config_file, env
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import http_get
from lib.utils_s3 import (download_and_check_md5, get_s3_file_md5,
                          load_to_s3_with_md5)

with DAG(
    dag_id='etl_import_dbsnp',
    start_date=datetime(2025, 8, 9),
    schedule='45 6 * * 6',
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    def _file():
        url = 'https://ftp.ncbi.nih.gov/snp/latest_release/VCF'

        # Get file version
        files = http_get(f'{url}').text
        matches = re.findall(r'GCF_000001405\.(\d+)\.gz\.tbi\.md5', files)

        if not matches:
            raise AirflowFailException('No dbsnp file found')

        version = matches[-1]
        file = f'GCF_000001405.{version}.gz.tbi'
        logging.info(f'Last dbsnp file version available : {file}')

        # Get latest release MD5 checksum
        md5_text = http_get(f'{url}/{file}.md5').text
        md5_hash = re.search('^([0-9a-f]+)', md5_text).group(1)
        logging.info(f'MD5 hash is: {md5_hash}')

        # Get latest s3 MD5 checksum
        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/dbsnp/{file}'
        s3_md5 = get_s3_file_md5(s3, s3_bucket, s3_key)
        if not s3_md5:
            logging.info(f'The file does not exist in S3: {s3_key}')
        else:
            logging.info(f'Current dbsnp file imported MD5 hash: {s3_md5}')

        # Skip task if up to date
        if s3_md5 == md5_hash:
            logging.info(f'The file is up to date!')
            raise AirflowSkipException()

        download_md5 = download_and_check_md5(url, file, md5_hash)

        # Upload file to S3
        load_to_s3_with_md5(s3, s3_bucket, s3_key, file, download_md5)
        logging.info(f'New dbsnp file imported MD5 hash: {md5_hash}')

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-dbsnp-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'dbsnp',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_dbsnp_table',
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
