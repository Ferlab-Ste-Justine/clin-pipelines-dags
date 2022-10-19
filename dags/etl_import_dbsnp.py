import logging
import re
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from lib import config
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import http_get
from lib.utils_import import get_s3_file_md5, download_and_check_md5, load_to_s3_with_md5


with DAG(
    dag_id='etl_import_dbsnp',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file():
        url = 'https://ftp.ncbi.nih.gov/snp/latest_release/VCF'
        file = 'GCF_000001405.39.gz.tbi'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/dbsnp/{file}'

        # Get latest release MD5 checksum
        md5_text = http_get(f'{url}/{file}.md5').text
        md5_hash = re.search('^([0-9a-f]+)', md5_text).group(1)

        # Get latest s3 MD5 checksum
        s3_md5 = get_s3_file_md5(s3, s3_bucket, s3_key)
        logging.info(f'Current imported MD5 hash: {s3_md5}')

        # Skip task if up to date
        if s3_md5 == md5_hash:
            raise AirflowSkipException()

        download_md5 = download_and_check_md5(url, file, md5_hash)

        # Upload file to S3
        load_to_s3_with_md5(s3, s3_bucket, s3_key, file, download_md5)
        logging.info(f'New imported MD5 hash: {md5_hash}')

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
        spark_config='enriched-etl',
        arguments=['dbsnp'],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
