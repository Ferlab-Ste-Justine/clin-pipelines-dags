import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.config import K8sContext, config_file, env
from lib.operators.spark import SparkOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks.params_validate import validate_color
from lib.utils import http_get, http_get_file
from lib.utils_etl import spark_jar
from lib.utils_s3 import get_s3_file_version, load_to_s3_with_version

with DAG(
    dag_id='etl_import_hpo_genes',
    start_date=datetime(2022, 8, 16),
    schedule='30 7 * * 6',
    params={
        'spark_jar': Param('', type=['null', 'string']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure
    },
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1
    ) as dag:

    def download(file, dest = None, **context):
        url = 'https://github.com/obophenotype/human-phenotype-ontology/releases'

        destFile = file if dest is None else dest

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/hpo/{destFile}'

        # Get imported version
        imported_ver = get_s3_file_version(s3, s3_bucket, s3_key)
        logging.info(f'{file} imported version: {imported_ver}')

        # Get latest version
        html = http_get(url).text
        latest_ver_search = re.search(f'/download/(v?.+)/{file}', html)

        if latest_ver_search is None:
            logging.error(f'Could not find source latest version for: {file}')
            context['ti'].xcom_push(key=f'{destFile}.version', value=imported_ver)
            raise AirflowFailException()

        latest_ver = latest_ver_search.group(1)
        logging.info(f'{file} latest version: {latest_ver}')

        # share the current version with other tasks
        context['ti'].xcom_push(key=f'{destFile}.version', value=latest_ver)

        # Skip task if up to date
        if imported_ver == latest_ver:
            raise AirflowSkipException()

        # Download file
        http_get_file(f'{url}/download/{latest_ver}/{file}', file)

        # Upload file to S3
        load_to_s3_with_version(s3, s3_bucket, s3_key, file, latest_ver)
        logging.info(f'New {file} imported version: {latest_ver}')


    download_hpo_genes = PythonOperator(
        task_id='download_hpo_genes',
        python_callable=download,
        op_args=['genes_to_phenotype.txt']
    )

    normalized_hpo_genes = SparkOperator(
        task_id='normalized_hpo_genes',
        name='etl-import-hpo-genes',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-medium',
        spark_jar=spark_jar(),
        arguments=[
            'hpo',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_import_hpo_genes',
        ],
    )

    trigger_genes = TriggerDagRunOperator(
        task_id='genes',
        trigger_dag_id='etl_import_genes',
        wait_for_completion=False,
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    chain(download_hpo_genes, normalized_hpo_genes, trigger_genes, slack)
