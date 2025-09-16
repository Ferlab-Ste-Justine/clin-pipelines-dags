import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.config import K8sContext, config_file, env
from lib.operators.spark import SparkOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks.public_data import update_public_data_entry_task
from lib.tasks.should_continue import should_continue, skip_if_not_new_version
from lib.utils_s3 import (download_and_check_md5, get_s3_file_md5,
                          load_to_s3_with_md5)

with DAG(
    dag_id='etl_import_orphanet',
    start_date=datetime(2025, 9, 6),
    schedule='15 6 * * 6#1',
    params={
        'skip_if_not_new_version': Param('yes', enum=['yes', 'no']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
    catchup=False,
    max_active_runs=1
) as dag:

    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)
    def file(**context):
        url = 'https://www.orphadata.com/data/xml'
        genes_file = 'en_product6.xml'
        diseases_file = 'en_product9_ages.xml'
        updated = False

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key_genes = f'raw/landing/orphanet/{genes_file}'
        s3_key_diseases = f'raw/landing/orphanet/{diseases_file}'

        # Get latest s3 MD5 checksum
        s3_md5_genes = get_s3_file_md5(s3, s3_bucket, s3_key_genes)
        logging.info(f'Current genes imported MD5 hash: {s3_md5_genes}')

        s3_md5_diseases = get_s3_file_md5(s3, s3_bucket, s3_key_diseases)
        logging.info(f'Current diseases imported MD5 hash: {s3_md5_diseases}')

        # Download file
        download_md5_genes = download_and_check_md5(url, genes_file, None)
        download_md5_diseases = download_and_check_md5(url, diseases_file, None)

        # Verify MD5 checksum
        if download_md5_genes != s3_md5_genes:
            # Upload file to S3
            load_to_s3_with_md5(s3, s3_bucket, s3_key_genes, genes_file, download_md5_genes)
            logging.info(f'New genes imported MD5 hash: {download_md5_genes}')
            updated = True

        # Verify MD5 checksum
        if download_md5_diseases != s3_md5_diseases:
            # Upload file to S3
            load_to_s3_with_md5(s3, s3_bucket, s3_key_diseases, diseases_file, download_md5_diseases)
            logging.info(f'New diseases imported MD5 hash: {download_md5_diseases}')
            updated = True

        # Skip task if up to date
        skip_if_not_new_version(updated, context)
       

    get_file = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-orphanet-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'orphanet',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_orphanet_table',
        ],
    )

    trigger_genes = TriggerDagRunOperator(
        task_id='genes',
        trigger_dag_id='etl_import_genes',
        wait_for_completion=False,
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    get_file >> should_continue() >> table >> trigger_genes >> update_public_data_entry_task('orphanet', True) >> slack
