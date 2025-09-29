from datetime import datetime
import re

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from lib.config import K8sContext, env, es_url, indexer_context
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.params_validate import validate_color
from lib.tasks.public_data import PublicSourceDag, should_continue
from lib.utils import http_get
from lib.utils_etl import (color, obo_parser_spark_jar, spark_jar)

hpo_terms_dag = PublicSourceDag(
    name='hpo_terms',
    schedule='15 7 * * 6',  # every Saturday at 7:15am
    add_to_file=False,
    raw_folder='hpo'
)

with DAG(
    dag_id='etl_import_hpo_terms',
    start_date=datetime(2022, 8, 16),
    schedule=hpo_terms_dag.schedule,
    params={
        'color': Param('', type=['null', 'string']),
        'spark_jar': Param('', type=['null', 'string']),
        'obo_parser_spark_jar': Param('', type=['null', 'string'])
    } | PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1
    ) as dag:

    params_validate = validate_color(color())
    env_color = params_validate.__str__()
    prefixed_color = ('_' + env_color) if env_color else ''

    @task(task_id='download_hpo_terms')
    def download():
        url = 'https://github.com/obophenotype/human-phenotype-ontology/releases'
        file_name = 'hp-fr.obo'

        # Get latest version
        hpo_terms_dag.set_last_version(re.search(f'/download/(v?.+)/{file_name}', http_get(url).text).group(1))
        # Upload files to S3 (if new)
        # not used for now but we could maybe update obo-parser to use that file as input instead of downloading the obo file
        hpo_terms_dag.upload_file_if_new(url=f'{url}/download/{hpo_terms_dag.last_version}/{file_name}', file_name='hp.obo')

        return hpo_terms_dag.serialize()

    dag_data = download()

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
            '{{ ti.xcom_pull(task_ids="download_hpo_terms", key="return_value")["last_version"] }}',
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
            'bio.ferlab.clin.etl.PublishHpoTerms', f'clin_{env}' + prefixed_color + '_hpo', '{{ ti.xcom_pull(task_ids="download_hpo_terms", key="return_value")["last_version"] }}', 'hpo'
        ],
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    params_validate >> dag_data >> should_continue(dag_data) >> normalized_hpo_terms >> index_hpo_terms >> publish_hpo_terms >> slack
