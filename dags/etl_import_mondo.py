import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.config import K8sContext, config_file, env, es_url, indexer_context
from lib.operators.curl import CurlOperator
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks import publish_index
from lib.tasks.params_validate import validate_color
from lib.utils import http_get, http_get_file
from lib.utils_etl import (batch_id, color, obo_parser_spark_jar, skip_import,
                           spark_jar)
from lib.utils_import import get_s3_file_version, load_to_s3_with_version

with DAG(
    dag_id='etl_import_mondo',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'color': Param('', type=['null', 'string']),
        'spark_jar': Param('', type=['null', 'string']),
        'obo_parser_spark_jar': Param('', type=['null', 'string']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure
    },
    max_active_tasks=1,
    max_active_runs=1
    ) as dag:

    params_validate = validate_color(color())

    def download(file, dest = None, **context):
        url = 'https://github.com/monarch-initiative/mondo/releases'

        destFile = file if dest is None else dest

        s3 = S3Hook(config.s3_conn_id)
        s3_bucket = f'cqgc-{env}-app-datalake'
        s3_key = f'raw/landing/mondo/{destFile}'

        # Get latest version
        html = http_get(url).text
        latest_ver = re.search(f'/download/(v.+)/{file}', html).group(1)
        logging.info(f'{file} latest version: {latest_ver}')

        # Get imported version
        imported_ver = get_s3_file_version(s3, s3_bucket, s3_key)
        logging.info(f'{file} imported version: {imported_ver}')

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


    # used to get the version, the obo-parser will download the file on its own again
    download_mondo_terms = PythonOperator(
        task_id='download_mondo_terms',
        python_callable=download,
        op_args=['mondo-base.obo', 'mondo.obo'],
        provide_context=True,
    )

    normalized_mondo_terms = SparkOperator(
        task_id='normalized_mondo_terms',
        name='etl-import-hpo-terms',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.HPOMain',
        spark_config='config-etl-medium',
        spark_jar=obo_parser_spark_jar(),
        arguments=[
            'https://github.com/monarch-initiative/mondo/releases/download/{{ ti.xcom_pull(task_ids="download_mondo_terms", key="mondo.obo.version") }}/mondo-base.obo',
            f'cqgc-{env}-app-datalake',
            'public/mondo_terms',
            'False',
            '',
        ],
    )

    index_mondo_terms = SparkOperator(
        task_id='index_mondo_terms',
        name='etl-index-terms',
        k8s_context=indexer_context,
        spark_class='bio.ferlab.clin.etl.es.Indexer',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar(),
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color('_') + '_mondo', #clin_qa_green_hpo_v2024-01-01
            '{{ ti.xcom_pull(task_ids="download_mondo_terms", key="mondo.obo.version") }}',
            'mondo_terms_template.json',
            'mondo_terms',
            '1900-01-01 00:00:00',
            f'config/{env}.conf',
        ],
    )

    publish_mondo = publish_index.mondo('{{ ti.xcom_pull(task_ids="download_mondo_terms", key="mondo.obo.version") }}', color('_'), spark_jar(), task_id='publish_mondo')

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    chain(params_validate, download_mondo_terms, normalized_mondo_terms, index_mondo_terms, publish_mondo, slack)
