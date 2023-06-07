import logging
import re
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from lib import config
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import http_get
from lib.utils_import import get_s3_file_md5, download_and_check_md5, load_to_s3_with_md5


with DAG(
    dag_id='etl_import_dbnsfp',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    # Following steps to get and build the raw zip file
    # 1 - download dbNSFP4.3a.zip from https://sites.google.com/site/jpopgen/dbNSFP
    # 2 - extract ZIP content on local inside a folder
    # 4 - deploy all files dbNSFP4.3a_variant.chr*.gz on S3

    raw = SparkOperator(
        task_id='raw',
        name='etl-import-dbnsfp-raw-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=[f'config/{env}.conf', 'default', 'dbnsfp_raw'],
        on_execute_callback=Slack.notify_dag_start,
    )

    enriched = SparkOperator(
        task_id='enriched',
        name='etl-import-dbnsfp-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=[f'config/{env}.conf', 'default', 'dbnsfp'],
        on_success_callback=Slack.notify_dag_completion,
    )

    raw >> enriched
