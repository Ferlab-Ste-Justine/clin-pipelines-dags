import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config
from lib.config import env, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import get_update_public_data_entry_task, push_version_to_xcom
from lib.utils import http_get, http_get_file
from lib.utils_s3 import get_s3_file_version, load_to_s3_with_version

with DAG(
    dag_id='etl_import_refseq_annotation',
    start_date=datetime(2022, 1, 1),
    schedule=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    def _file(**context):
        url = 'https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/annotation_releases/current'
        file = 'GCF_GRCh38_genomic.gff.gz'

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/refseq/{file}'

        # Get latest version
        html = http_get(url).text
        latest_ver = re.search('>GCF_(.+_GRCh38.+)/<', html).group(1)
        logging.info(f'RefSeq Annotation latest version: {latest_ver}')

        # Get imported version
        imported_ver = get_s3_file_version(s3, s3_bucket, s3_key)
        logging.info(f'RefSeq Annotation imported version: {imported_ver}')

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        # Download file
        http_get_file(
            f'{url}/GCF_{latest_ver}/GCF_{latest_ver}_genomic.gff.gz', file
        )

        # Upload file to S3
        load_to_s3_with_version(s3, s3_bucket, s3_key, file, latest_ver)
        logging.info(f'New RefSeq Annotation imported version: {latest_ver}')

        push_version_to_xcom(latest_ver, context)

    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start,
    )

    table = SparkOperator(
        task_id='table',
        name='etl-import-refseq-annotation-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'refseq_annotation',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_refseq_annotation_table',
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> table >> get_update_public_data_entry_task('refseq_annotation')
