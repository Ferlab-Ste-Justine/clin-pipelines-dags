import logging
from datetime import datetime
from itertools import chain

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from lib.config import env, s3_conn_id, basespace_illumina_credentials, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.utils import http_get
from lib.utils_import import stream_upload_to_s3, get_s3_file_version

with DAG(
        dag_id='etl_import_spliceai',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        max_active_tasks=1,  # Only one task can be scheduled at a time
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
) as dag:
    def _file():
        # file_name -> file_id
        indel = {
            "spliceai_scores.raw.indel.hg38.vcf.gz": 16525003580,
            "spliceai_scores.raw.indel.hg38.vcf.gz.tbi": 16525276839
        }
        snv = {
            "spliceai_scores.raw.snv.hg38.vcf.gz": 16525380715,
            "spliceai_scores.raw.snv.hg38.vcf.gz.tbi": 16525505189
        }

        s3 = S3Hook(s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        updated = False

        def s3_key(file_name):
            return f'raw/landing/spliceai/{file_name}'

        def download_url(id):
            return f'https://api.basespace.illumina.com/v1pre3/files/{id}/content'

        def ver_url(id):
            return f'https://api.basespace.illumina.com/v1pre3/files/{id}'

        for file_name, file_id in chain(indel.items(), snv.items()):
            header = {'x-access-token': f'{basespace_illumina_credentials}'}

            # Get current imported S3 version
            imported_ver = get_s3_file_version(s3, s3_bucket, s3_key(file_name))
            logging.info(f'Current file {file_name} imported version: {imported_ver}')

            # Get latest available version
            latest_ver = http_get(ver_url(file_id), header).json()['Response']['ETag']
            logging.info(f'File {file_name} latest available version: {latest_ver}')

            # Skip task if up to date
            if imported_ver == latest_ver:
                continue

            # Download file
            stream_upload_to_s3(s3, s3_bucket, s3_key(file_name), download_url(file_id), header, replace=True)
            logging.info(f'File {file_name} uploaded to S3')

            # Upload version to S3
            s3.load_string(latest_ver, f'{s3_key(file_name)}.version', s3_bucket, replace=True)
            updated = True

        # If no files have been updated, skip task
        if not updated:
            raise AirflowSkipException()


    file = PythonOperator(
        task_id='file',
        python_callable=_file,
        on_execute_callback=Slack.notify_dag_start
    )

    indel_table = SparkOperator(
        task_id='indel_table',
        name='etl-import-spliceai-indel-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'spliceai_indel',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_spliceai_indel_table',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    snv_table = SparkOperator(
        task_id='snv_table',
        name='etl-import-spliceai-snv-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'spliceai_snv',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_spliceai_snv_table',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    enrich_indel = SparkOperator(
        task_id='enrich_indel',
        name='etl-enrich-spliceai-indel',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-turbo',
        arguments=[
            'spliceai_enriched_indel',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_enrich_spliceai_indel',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    enrich_snv = SparkOperator(
        task_id='enrich_snv',
        name='etl-enrich-spliceai-snv',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'spliceai_enriched_snv',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_enrich_spliceai_snv',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    file >> [indel_table, snv_table]
    indel_table >> enrich_indel
    snv_table >> enrich_snv
    [enrich_snv, enrich_indel] >> slack
