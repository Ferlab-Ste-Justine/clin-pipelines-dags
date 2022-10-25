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
from lib.utils import http_get_file
from lib.utils_import import get_s3_file_version, load_to_s3_with_version


with DAG(
    dag_id='etl_import_ddd',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file():
        url = 'https://www.ebi.ac.uk/gene2phenotype/downloads'
        file = 'DDG2P.csv.gz'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/ddd/{file}'

        # Download file
        http_get_file(f'{url}/{file}', file)

        # Get latest version
        file = open(file, 'rb')
        first_line = str(file.readline())
        file.close()
        latest_ver = re.search('DDG2P_([0-9_]+)\.csv', first_line).group(1)
        logging.info(f'DDD latest version: {latest_ver}')

        # Get imported version
        imported_ver = get_s3_file_version(s3, s3_bucket, s3_key)
        logging.info(f'DDD imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        # Upload file to S3
        load_to_s3_with_version(s3, s3_bucket, s3_key, file)
        logging.info(f'New DDD imported version: {latest_ver}')

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-ddd-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=['ddd'],
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
