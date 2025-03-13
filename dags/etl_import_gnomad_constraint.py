import logging
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config
from lib.config import env, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import http_get_file
from lib.utils_s3 import get_s3_file_version, load_to_s3_with_version

with DAG(
        dag_id='etl_import_gnomad_constraint',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        }
) as dag:
    def _file():
        # Get latest version
        latest_ver = '2.1.1'
        logging.info(f'gnomAD constraint metrics latest version: {latest_ver}')

        url = f'https://gnomad-public-us-east-1.s3.amazonaws.com/release/{latest_ver}/constraint'
        file = f'gnomad.v{latest_ver}.lof_metrics.by_gene.txt.bgz'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'/raw/landing/gnomad_v{latest_ver.replace(".", "_")}/{file.replace("bgz", "gz")}'

        # Get imported version
        imported_ver = get_s3_file_version(s3, s3_bucket, s3_key)
        logging.info(f'gnomAD constraint metrics imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        # Download file
        http_get_file(f'{url}/{file}', file)

        # Upload file to S3
        load_to_s3_with_version(s3, s3_bucket, s3_key, file, latest_ver)
        logging.info(f'New gnomAD constraint metrics imported version: {latest_ver}')


    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-gnomad-constraint',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'gnomad_constraint',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_gnomad_constraint',
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
