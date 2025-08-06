import logging
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.config import K8sContext, config_file, env
from lib.operators.spark import SparkOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.utils_s3 import (download_and_check_md5, get_s3_file_md5,
                          load_to_s3_with_md5)

with DAG(
    dag_id='etl_import_omim',
    start_date=datetime(2025, 8, 9),
    schedule='30 6 * * 6',
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
    catchup=False,
    max_active_runs=1
) as dag:

    def _file():
        url = 'https://data.omim.org/downloads/oOHLFM8TQEaUch03i8MF7A'
        genes_file = 'genemap2.txt'
        updated = False

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key_genes = f'raw/landing/omim/{genes_file}'

        # Get latest s3 MD5 checksum
        s3_md5_genes = get_s3_file_md5(s3, s3_bucket, s3_key_genes)
        logging.info(f'Current OMIM genes imported MD5 hash: {s3_md5_genes}')

        # Download file
        download_md5_genes = download_and_check_md5(url, genes_file, None)

        # Verify MD5 checksum
        if download_md5_genes != s3_md5_genes:
            # Upload file to S3
            load_to_s3_with_md5(s3, s3_bucket, s3_key_genes, genes_file, download_md5_genes)
            logging.info(f'New OMIM genes imported MD5 hash: {download_md5_genes}')
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
        name='etl-import-omim-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'omim',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_omim_table',
        ],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_execute_callback=Slack.notify_dag_start,
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
     
    table >> trigger_genes >> slack
