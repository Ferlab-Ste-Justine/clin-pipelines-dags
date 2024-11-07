import logging
from datetime import datetime

import boto3

from botocore import UNSIGNED
from botocore.config import Config

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config
from lib.config import clin_datalake_bucket, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils_import import get_s3_file_version, stream_upload_to_s3

LATEST_VERSION = '4.1'
GNOMAD_S3_BUCKET = 'gnomad-public-us-east-1'
GNOMAD_S3_CONSTRANT_FILE = f'gnomad.v{LATEST_VERSION}.constraint_metrics.tsv'
GNOMAD_S3_PREFIX = f'release/{LATEST_VERSION}/constraint/{GNOMAD_S3_CONSTRANT_FILE}'
DESTINATION = f'/raw/landing/gnomad_v{LATEST_VERSION.replace(".", "_")}/gnomad.v{LATEST_VERSION}.constraint_metrics.tsv'

with DAG(
        dag_id='etl_import_gnomad_constraint',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        }
) as dag:
    def _file():
        logging.info(f'gnomAD constraint metrics latest version: {LATEST_VERSION}')

        s3_client_for_gnomad = boto3.client("s3", config=Config(signature_version=UNSIGNED))

        s3 = S3Hook(config.s3_conn_id)

        # Get imported version
        imported_ver = get_s3_file_version(s3, clin_datalake_bucket, DESTINATION)
        logging.info(f'gnomAD constraint metrics imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == LATEST_VERSION:
            raise AirflowSkipException()

        generation_params = {'Bucket': GNOMAD_S3_BUCKET, 'Key': GNOMAD_S3_PREFIX}
        presigned_url = s3_client_for_gnomad.generate_presigned_url("get_object", Params=generation_params)

        logging.info(f'Importing file {GNOMAD_S3_CONSTRANT_FILE}')
        stream_upload_to_s3(s3, clin_datalake_bucket, DESTINATION, presigned_url)

        # Update version
        logging.info(f'Version {LATEST_VERSION} of gnomAD constraint imported to S3.')
        s3.load_string(LATEST_VERSION, f'{DESTINATION}.version', clin_datalake_bucket, replace=True)

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