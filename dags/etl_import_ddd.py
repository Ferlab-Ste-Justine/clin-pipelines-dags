import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib import config
from lib.config import K8sContext, config_file, env
from lib.operators.spark import SparkOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks.public_data import update_public_data_entry_task
from lib.utils import http_get_file
from lib.utils_s3 import get_s3_file_version, load_to_s3_with_version

with DAG(
    dag_id='etl_import_ddd',
    start_date=datetime(2022, 1, 1),
    schedule=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
    catchup=False,
    max_active_runs=1
) as dag:

    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)  
    def file():
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
        load_to_s3_with_version(s3, s3_bucket, s3_key, file, latest_ver)
        logging.info(f'New DDD imported version: {latest_ver}')

        return latest_ver

    version = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-ddd-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'ddd',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_ddd_table',
        ],
        on_success_callback=Slack.notify_dag_completion,
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

    version >> table >> trigger_genes >> update_public_data_entry_task('gene2phenotype-ddd', version) >> slack
