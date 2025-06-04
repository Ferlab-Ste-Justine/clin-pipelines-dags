import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib import config
from lib.config import K8sContext, config_file, env
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import file_md5, http_get, http_get_file
from lib.utils_s3 import get_s3_file_version, load_to_s3_with_version

with DAG(
    dag_id='etl_import_clinvar',
    start_date=datetime(2022, 1, 1),
    schedule='0 21 * * 5',
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
    catchup=False,
    max_active_runs=1
) as dag:

    def _file():
        url = 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38'
        file = 'clinvar.vcf.gz'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/clinvar/{file}'

        # Get MD5 checksum
        md5_text = http_get(f'{url}/{file}.md5').text
        md5_hash = re.search('^([0-9a-f]+)', md5_text).group(1)

        # Get latest version
        latest_ver = re.search('clinvar_([0-9]+)\.vcf', md5_text).group(1)
        logging.info(f'ClinVar latest version: {latest_ver}')

        # Get imported version
        imported_ver = get_s3_file_version(s3, s3_bucket, s3_key)
        logging.info(f'ClinVar imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        # Download file
        http_get_file(f'{url}/{file}', file)

        # Verify MD5 checksum
        download_md5_hash = file_md5(file)
        if download_md5_hash != md5_hash:
            raise AirflowFailException('MD5 checksum verification failed')

        # Upload file to S3
        load_to_s3_with_version(s3, s3_bucket, s3_key, file, latest_ver)
        logging.info(f'New ClinVar imported version: {latest_ver}')

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-clinvar-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'clinvar',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_clinvar_table',
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
