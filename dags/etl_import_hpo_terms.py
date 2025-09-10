import logging
import re
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.tasks.should_continue import should_continue, skip_if_not_new_version
from lib.config import K8sContext, env, es_url, indexer_context
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.params_validate import validate_color
from lib.utils import http_get, http_get_file
from lib.utils_etl import (color, obo_parser_spark_jar, spark_jar)
from lib.utils_s3 import get_s3_file_version, load_to_s3_with_version

with DAG(
    dag_id='etl_import_hpo_terms',
    start_date=datetime(2022, 8, 16),
    schedule='15 7 * * 6',
    params={
        'color': Param('', type=['null', 'string']),
        'spark_jar': Param('', type=['null', 'string']),
        'obo_parser_spark_jar': Param('', type=['null', 'string']),
        'skip_if_not_new_version': Param('yes', enum=['yes', 'no']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure
    },
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1
    ) as dag:

    params_validate = validate_color(color())
    env_color = params_validate.__str__()
    prefixed_color = ('_' + env_color) if env_color else ''

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
        skip_if_not_new_version(imported_ver != latest_ver, context)

        # Download file
        http_get_file(f'{url}/download/{latest_ver}/{file}', file)

        # Upload file to S3
        load_to_s3_with_version(s3, s3_bucket, s3_key, file, latest_ver)
        logging.info(f'New {file} imported version: {latest_ver}')

    # not used for now but we could maybe update obo-parser to use that file as input instead of downloading the obo file
    download_hpo_terms = PythonOperator(
        task_id='download_hpo_terms',
        python_callable=download,
        op_args=['hp-fr.obo', 'hp.obo']
    )

    normalized_hpo_terms = SparkOperator(
        task_id='normalized_hpo_terms',
        name='etl-import-hpo-terms',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.HPOMain',
        spark_config='config-etl-medium',
        spark_jar=obo_parser_spark_jar(),
        arguments=[
            'https://raw.githubusercontent.com/obophenotype/human-phenotype-ontology/master/hp.obo',
            f'cqgc-{env}-app-datalake',
            'public/hpo_terms',
            'False',
            '',
        ],
    )

    index_hpo_terms = SparkOperator(
        task_id='index_hpo_eterms',
        name='etl-index-terms',
        k8s_context=indexer_context,
        spark_class='bio.ferlab.clin.etl.es.Indexer',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar(),
        arguments=[
            es_url, '', '',
            f'clin_{env}' + prefixed_color + '_hpo', #clin_qa_green_hpo_v2024-01-01
            '{{ ti.xcom_pull(task_ids="download_hpo_terms", key="hp.obo.version") }}',
            'hpo_terms_template.json',
            'hpo_terms',
            '1900-01-01 00:00:00',
            f'config/{env}.conf',
        ],
    )

    # will update FHIR and switch alias in ES
    publish_hpo_terms = PipelineOperator(
        task_id='publish_hpo_terms',
        name='etl-publish-hpo-terms',
        k8s_context=K8sContext.DEFAULT,
        color=env_color,
        arguments=[
            'bio.ferlab.clin.etl.PublishHpoTerms', f'clin_{env}' + prefixed_color + '_hpo', '{{ ti.xcom_pull(task_ids="download_hpo_terms", key="hp.obo.version") }}', 'hpo'
        ],
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    chain(params_validate, download_hpo_terms, should_continue(), normalized_hpo_terms, index_hpo_terms, publish_hpo_terms, slack)
