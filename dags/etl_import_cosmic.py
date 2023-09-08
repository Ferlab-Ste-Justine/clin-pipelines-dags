import base64
import logging
import re
import tarfile
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

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
    def download_file(url, path, ver, file_name, credentials):
        download_url = http_get(
            f'{url}/{path}/{ver}/{file_name}',
            {'Authorization': f'Basic {credentials}'}
        ).json()['url']
        http_get_file(download_url, file_name)


    def _files():
        url = 'https://cancer.sanger.ac.uk/cosmic'
        path = 'file_download/GRCh38/cosmic'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'

        # Get latest version
        html = http_get(url).text
        latest_ver = re.search('COSMIC (v[0-9]+),', html).group(1)
        logging.info(f'COSMIC latest version: {latest_ver}')

        # TODO: Download Cosmic_CancerGeneCensus_GRCh38.tar when scripted downloads are added to new download page
        # gene_census_file = 'cancer_gene_census.csv'
        mutation_census_file = 'cmc_export.tsv.gz'
        mutation_census_archive = 'CMC.tar'
        updated = False

        for file_name in [mutation_census_file]:
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
            if file_name == mutation_census_file:
                download_file(url, path, latest_ver, mutation_census_archive, credentials)
            else:
                download_file(url, path, latest_ver, file_name, credentials)

            # Extract mutation census file
            if file_name == mutation_census_file:
                with tarfile.open(mutation_census_archive, 'r') as tar:
                    tar.extract(mutation_census_file)

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

    gene_table = SparkOperator(
        task_id='gene_table',
        name='etl-import-cosmic-gene-set-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=[
            'cosmic_gene_set',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_import_cosmic_gene_set_table',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    mutation_table = SparkOperator(
        task_id='mutation_table',
        name='etl-import-cosmic-mutation-set-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=[
            'cosmic_mutation_set',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_cosmic_mutation_set_table',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    files >> [gene_table, mutation_table] >> slack
