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
from lib.utils_s3 import get_s3_file_md5, download_and_check_md5, load_to_s3_with_md5

with DAG(
    dag_id='etl_import_orphanet',
    start_date=datetime(2022, 1, 1),
    schedule=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file():
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
            load_to_s3_with_md5(s3, s3_bucket, s3_key_genes, file, download_md5_genes)
            logging.info(f'New genes imported MD5 hash: {download_md5_genes}')
            updated = True

        # Verify MD5 checksum
        if download_md5_diseases != s3_md5_diseases:
            # Upload file to S3
            load_to_s3_with_md5(s3, s3_bucket, s3_key_diseases, file, download_md5_diseases)
            logging.info(f'New diseases imported MD5 hash: {download_md5_diseases}')
            updated = True

        if not updated:
            raise AirflowSkipException()
       

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

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
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
