import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from lib import config
from lib.config import env, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import update_public_data_entry_task
from lib.tasks.should_continue import should_continue, skip_if_not_new_version
from lib.utils_s3 import get_s3_file_version, download_and_check_md5, load_to_s3_with_version

with DAG(
    dag_id='etl_import_ensembl',
    start_date=datetime(2022, 1, 1),
    schedule='45 7 * * 6',
    params={
        'skip_if_not_new_version': Param('yes', enum=['yes', 'no']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
    catchup=False,
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

    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)
    def file(**context):
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

        # Skip task if up to date
        skip_if_not_new_version(updated, context)

        return new_version

    version = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-ensembl-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'ensembl_mapping',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_ensembl_table',
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    version >> should_continue() >> table >> update_public_data_entry_task('ensembl', version)
