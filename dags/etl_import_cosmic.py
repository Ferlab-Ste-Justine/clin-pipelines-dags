import base64
import logging
import re
import tarfile
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config
from lib.config import env, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import http_get, http_get_file
from lib.utils_import import get_s3_file_version, load_to_s3_with_version

with DAG(
        dag_id='etl_import_cosmic',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
) as dag:
    def get_latest_ver(url):
        html = http_get(url).text
        latest_ver = re.search('COSMIC (v[0-9]+),', html).group(1)
        logging.info(f'COSMIC latest version: {latest_ver}')
        return latest_ver


    def _files():
        url = 'https://cancer.sanger.ac.uk/cosmic'
        path = 'file_download/GRCh38/cosmic'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'

        # Get latest version
        html = http_get(url).text
        latest_ver = re.search('COSMIC (v[0-9]+),', html).group(1)
        logging.info(f'COSMIC latest version: {latest_ver}')

        gene_census_file = 'cancer_gene_census.csv'
        mutation_census_file = 'CMC.tar'
        updated = False

        for file_name in [gene_census_file, mutation_census_file]:
            s3_key = f'raw/landing/cosmic/{file_name}'

            # Get imported version
            imported_ver = get_s3_file_version(s3, s3_bucket, s3_key)
            logging.info(f'Current file {file_name} imported version: {imported_ver}')

            # Skip task if up to date
            if imported_ver == latest_ver:
                f'Skipping import of file {file_name}. Imported version is up to date.'
                continue

            # Encode credentials
            credentials = base64.b64encode(
                config.cosmic_credentials.encode()
            ).decode()

            # Get download url
            download_url = http_get(
                f'{url}/{path}/{latest_ver}/{file_name}',
                {'Authorization': f'Basic {credentials}'}
            ).json()['url']

            # Download file
            http_get_file(download_url, file_name)

            # Extract mutation census file
            if file_name == mutation_census_file:
                extracted_file_name = 'cmc_export.tsv.gz'
                with tarfile.open(file_name, 'r') as tar:
                    tar.extract(extracted_file_name)
                file_name = extracted_file_name

            # Upload file to S3
            load_to_s3_with_version(s3, s3_bucket, s3_key, file_name, latest_ver)
            logging.info(f'New file {file_name} imported version: {latest_ver}')
            updated = True

        # If no files have been updated, skip task
        if not updated:
            raise AirflowSkipException()


    files = PythonOperator(
        task_id='files',
        python_callable=_files,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-cosmic-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.public.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=[
            'cosmic_gene_set',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_cosmic_table',
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    # TODO: Import mutation census public table

    files >> table
