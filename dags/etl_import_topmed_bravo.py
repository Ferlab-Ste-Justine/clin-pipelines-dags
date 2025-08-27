import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from lib import config
from lib.config import env, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import update_public_data_entry_task
from lib.tasks.should_continue import should_continue, skip_if_not_new_version
from lib.utils import http_get
from lib.utils_s3 import get_s3_file_version, stream_upload_or_resume_to_s3

with DAG(
    dag_id='etl_import_topmed_bravo',
    start_date=datetime(2022, 1, 1),
    schedule='0 7 * */3 6',
    catchup=False,
    params={
        'freeze_version': Param(None, type=['null', 'integer']),
        'credential': Param('', type=['null', 'string']),
        'skip_if_not_new_version': Param('yes', enum=['yes', 'no']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
    max_active_tasks=8
) as dag:

    download_link_url = 'https://api.bravo.sph.umich.edu/ui/link?chrom=chr'
    file_prefix = 'bravo-dbsnp-chr'
    version_filename= 'bravo-dbsnp-freeze'
    file_ext = '.vcf.gz'

    s3 = S3Hook(config.s3_conn_id)
    s3_bucket = f'cqgc-{env}-app-datalake'
    s3_key_prefix = f'raw/landing/topmed/'

    chromosomes = list(range(1, 22)) + list('X')

    with TaskGroup(group_id='files') as files:

        @task(task_id='init', on_execute_callback=Slack.notify_dag_start)
        def init(**context):
            latest_ver = str(context["params"]["freeze_version"])

            # Get imported version
            imported_ver = get_s3_file_version(s3, s3_bucket, f'{s3_key_prefix}{version_filename}')
            logging.info(f'TOPMed Bravo imported version: {imported_ver}')

            # Skip task if up to date
            skip_if_not_new_version(imported_ver != latest_ver, context)

            # Send latest version to xcom
            return latest_ver

        version = init()

        @task
        def download(version, chromosome: str, **context):
            # Skip download if no new version
            if not version:
                raise AirflowSkipException()

            cookie = context["params"]["credential"]
            if not cookie:
                raise AirflowFailException('No TOPMed Bravo credentials provided (should be a valid cookie)')

            filename = f'{file_prefix}{chromosome}{file_ext}'

            # Get file link
            response = http_get(f'{download_link_url}{chromosome}', headers={'Cookie': cookie})
            download_link = response.json()['url']

            # Upload file directly to S3
            stream_upload_or_resume_to_s3(s3, s3_bucket, f'{s3_key_prefix}{filename}', download_link,
                {
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Cookie': cookie,
                },
            )
            logging.info('Upload to S3 complete')

        variants = download.expand(version= version, chromosome=chromosomes)

        @task(task_id='release')
        def release(version):
            s3.load_string(version, f'{s3_key_prefix}{version_filename}.version', s3_bucket, replace=True)
            logging.info(f'New TOPMed Bravo imported version: {version}')

        version >> should_continue() >> variants >> release(version)

    table = SparkOperator(
        task_id='table',
        name='etl-import-topmed-bravo-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'topmed_bravo',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_topmed_bravo_table'
        ],
        on_success_callback=Slack.notify_dag_completion,
    )


    files >> table >> update_public_data_entry_task(version)
