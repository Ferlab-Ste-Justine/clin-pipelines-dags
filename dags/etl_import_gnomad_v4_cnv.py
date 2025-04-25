import logging
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.config import K8sContext, config_file, env
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import http_get_file
from lib.utils_etl import spark_jar
from lib.utils_s3 import get_s3_file_version, load_to_s3_with_version

with DAG(
    dag_id='etl_import_gnomad_v4_cnv',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'spark_jar': Param('', type=['null', 'string']),
        },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure
    },
    max_active_tasks=1,
    max_active_runs=1
) as dag:
    def _file():
        # Get latest version
        latest_ver = '4.1'
        logging.info(f'gnomAD CNV latest version: {latest_ver}')

        url = f'https://gnomad-public-us-east-1.s3.amazonaws.com/release/{latest_ver}/exome_cnv'
        file = f'gnomad.v{latest_ver}.cnv.all.vcf.gz'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'/raw/landing/gnomad_v{latest_ver}/exome_cnv/{file}'

        # Get imported version
        imported_ver = get_s3_file_version(s3, s3_bucket, s3_key)
        logging.info(f'gnomAD CNV imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        # Download file
        http_get_file(f'{url}/{file}', file)

        # Upload file to S3
        load_to_s3_with_version(s3, s3_bucket, s3_key, file, latest_ver)
        logging.info(f'New gnomAD CNV imported version: {latest_ver}')


    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl_import_gnomad_v4_cnv',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'gnomadv4cnv',
            '--config',
            config_file,
            '--steps',
            'default',
            '--app-name',
            'etl_import_gnomad_v4_cnv',
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
