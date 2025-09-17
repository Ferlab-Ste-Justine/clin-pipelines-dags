import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config
from lib.config import env, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import update_public_data_entry_task
from lib.utils import http_get_file
from lib.utils_s3 import get_s3_file_version, load_to_s3_with_version

with DAG(
    dag_id='etl_import_1000_genomes',
    start_date=datetime(2022, 1, 1),
    schedule=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)
    def file():
        url = 'http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release'
        file = 'ALL.wgs.phase3_shapeit2_mvncall_integrated_v5c.20130502.sites.vcf.gz'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'/raw/landing/1000Genomes/{file}'

        # Get latest version
        latest_ver = '20130502'
        logging.info(f'1000 Genomes latest version: {latest_ver}')

        # Get imported version
        imported_ver = get_s3_file_version(s3, s3_bucket, s3_key)
        logging.info(f'1000 Genomes imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        # Download file
        http_get_file(f'{url}/{latest_ver}/{file}', file)

        # Upload file to S3
        load_to_s3_with_version(s3, s3_bucket, s3_key, file, latest_ver)
        logging.info(f'New 1000 Genomes imported version: {latest_ver}')

        return latest_ver

    version = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-1000-genomes-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        on_success_callback=Slack.notify_dag_completion,
        arguments=[
            '1000genomes',
            '--config', config_file,
            '--steps', 'default'
        ]
    )
    
    version >> table >> update_public_data_entry_task(version)
