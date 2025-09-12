import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup

from lib import config
from lib.config import env, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import update_public_data_entry_task, push_version_to_xcom
from lib.utils import http_get, http_get_file
from lib.utils_s3 import get_s3_file_version

with DAG(
    dag_id='etl_import_topmed_bravo',
    start_date=datetime(2022, 1, 1),
    schedule=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    url = 'https://bravo.sph.umich.edu'
    path = 'hg38/downloads'
    file_prefix = 'bravo-dbsnp-chr'
    file_ext = '.vcf.gz'

    s3 = S3Hook(config.s3_conn_id)
    s3_bucket = f'cqgc-{env}-app-datalake'
    s3_key_prefix = f'raw/landing/topmed/'

    chromosomes = list(range(1, 23)) + list('X')

    with TaskGroup(group_id='files') as files:

        def _init(**context):
            # Get latest version
            html = http_get(url).text
            latest_ver = re.search(
                'bravo.sph.umich.edu/(.+)/hg38/', html).group(1)
            logging.info(f'TOPMed Bravo latest version: {latest_ver}')

            # Get imported version
            imported_ver = get_s3_file_version(
                s3, s3_bucket, f'{s3_key_prefix}{file_prefix}'
            )
            logging.info(f'TOPMed Bravo imported version: {imported_ver}')

            # Skip task if up to date
            if imported_ver == latest_ver:
                raise AirflowSkipException()

            # Pass the version to the next task
            push_version_to_xcom(latest_ver, context)

            # Send latest version to xcom
            return latest_ver

        init = PythonOperator(
            task_id='init',
            python_callable=_init,
            on_execute_callback=Slack.notify_dag_start,
        )

        def transfer(latest_ver: str, chromosome: str, coverage: bool = False):
            type_path = 'coverage' if coverage else 'vcf'
            file_suffix = '.coverage' if coverage else '.coverage'

            # Download file
            http_get_file(
                f'{url}/{latest_ver}/{path}/{type_path}/{chromosome}',
                f'{file_prefix}{chromosome}{file_suffix}{file_ext}',
                {
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Cookie': config.topmed_bravo_credentials,
                },
            )
            logging.info('Download complete')

            # Upload file to S3
            s3.load_file(
                f'{file_prefix}{chromosome}{file_suffix}{file_ext}',
                f'{s3_key_prefix}{file_prefix}{chromosome}{file_suffix}{file_ext}',
                s3_bucket, replace=True,
            )
            logging.info('Upload to S3 complete')

        @task
        def files_variants(chromosome: str, **kwargs):
            latest_ver = kwargs['ti'].xcom_pull(task_ids=['files.init'])[0]
            transfer(latest_ver, chromosome)

        variants = files_variants.expand(chromosome=chromosomes)

        @task
        def files_coverage(chromosome: str, **kwargs):
            latest_ver = kwargs['ti'].xcom_pull(task_ids=['files.init'])[0]
            transfer(latest_ver, chromosome, True)

        coverage = files_coverage.expand(chromosome=chromosomes)

        def _release(**kwargs):
            latest_ver = kwargs['ti'].xcom_pull(task_ids=['files.init'])[0]
            s3.load_string(
                latest_ver, f'{s3_key_prefix}{file_prefix}.version', s3_bucket, replace=True
            )
            logging.info(f'New TOPMed Bravo imported version: {latest_ver}')

        release = PythonOperator(
            task_id='release',
            python_callable=_release,
        )

        init >> variants >> coverage >> release

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


    files >> table >> update_public_data_entry_task('topmed_bravo')
