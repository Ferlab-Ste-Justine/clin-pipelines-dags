import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib import config
from lib.config import K8sContext, config_file, omim_credentials
from lib.operators.spark import SparkOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue
from lib.utils_s3 import download_and_check_md5, get_s3_file_md5


omim_dag = PublicSourceDag(
    name='omim',
    display_name="OMIM",
    website="https://www.omim.org/",
    schedule='30 6 * * 6',  # every Saturday at 6:30am
)

with DAG(
    dag_id=omim_dag.dag_id,
    start_date=datetime(2025, 8, 9),
    schedule=omim_dag.schedule,
    params=PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    catchup=False,
    max_active_runs=1
) as dag:

    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)
    def file():
        url = f'https://data.omim.org/downloads/{omim_credentials}'
        file_name = 'genemap2.txt'

        s3 = S3Hook(config.s3_conn_id)

        # Get latest s3 MD5 checksum
        s3_md5_genes = get_s3_file_md5(s3, omim_dag.s3_bucket, f'{omim_dag.s3_key}/{file_name}')
        logging.info(f'Current OMIM genes imported MD5 hash: {s3_md5_genes}')

        # Download file
        download_md5_genes = download_and_check_md5(url, file_name, None)

        omim_dag.is_new_version = s3_md5_genes != download_md5_genes
        if not omim_dag.is_new_version:
            logging.info('The file is up to date!')
        else:
            logging.info('The file has a new version, saving...')
            omim_dag.save_file(file_name, check_version=False, save_md5=True)

        return omim_dag

       
    dag_data = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-omim-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'omim',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_omim_table',
        ]
    )

    trigger_genes = TriggerDagRunOperator(
        task_id='genes',
        trigger_dag_id='etl_import_genes',
        wait_for_completion=False,
    )

    dag_data >> should_continue(dag_data) >> table >> trigger_genes >> update_public_data_info(dag_data)
