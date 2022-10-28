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
from lib.utils_import import get_s3_file_version, download_and_check_md5, load_to_s3_with_version


with DAG(
    dag_id='etl_import_ensembl',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def find_last_version(checksums: str, type: str) -> str:
        file = open(checksums, 'r')
        lines = file.readlines()
        file.close()
        for line in lines:
            version = re.search(f'Homo_sapiens.GRCh38.([0-9_]+)\.{type}.tsv.gz', line)
            if version is not None:
                return version.group(1)
        return None


    def _file():
        url = 'http://ftp.ensembl.org/pub/current_tsv/homo_sapiens'
        types = ['canonical', 'ena', 'entrez', 'refseq', 'uniprot']
        checksums = 'CHECKSUMS'
        updated = False

        download_and_check_md5(url, checksums, None)
  
        for type in types:

            file = f'Homo_sapiens.GRCh38.{type}.tsv.gz' # without version

            s3 = S3Hook(config.s3_conn_id)
            s3_bucket = f'cqgc-{env}-app-datalake'
            s3_key = f'raw/landing/ensembl/{file}'

            # Get latest s3 version
            s3_version = get_s3_file_version(s3, s3_bucket, s3_key)
            logging.info(f'Current {type} imported version: {s3_version}')

            new_version = find_last_version(checksums, type)

            if s3_version != new_version:
                # Download file with version
                file_with_version = f'Homo_sapiens.GRCh38.{new_version}.{type}.tsv.gz'
                download_and_check_md5(url, file_with_version, None)

                # Upload file to S3
                load_to_s3_with_version(s3, s3_bucket, s3_key, file_with_version, new_version)
                logging.info(f'New {type} imported version: {new_version}')
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
        name='etl-import-ensembl-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=['ensembl_mapping'],
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table
