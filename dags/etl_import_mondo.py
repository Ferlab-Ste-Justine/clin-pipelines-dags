from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from lib.config import K8sContext, env, es_url, indexer_context
from lib.operators.spark import SparkOperator
from lib.tasks import publish_index
from lib.tasks.params_validate import validate_color
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue
from lib.utils_etl import (color, obo_parser_spark_jar,
                           spark_jar)


mondo_dag = PublicSourceDag(
    name='mondo',
    display_name="Mondo Disease Ontology",
    website="https://mondo.monarchinitiative.org/",
    schedule='30 8 * * 6',  # every Saturday at 8:30am
)

with DAG(
    dag_id=mondo_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    schedule=None,
    params={
        'color': Param('', type=['null', 'string']),
        'spark_jar': Param('', type=['null', 'string']),
        'obo_parser_spark_jar': Param('', type=['null', 'string']),
    } | PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    max_active_tasks=1,
    max_active_runs=1
    ) as dag:

    params_validate = validate_color(color())

    @task(task_id='download_mondo_terms')
    def download():
        url = 'https://github.com/monarch-initiative/mondo/releases'
        file_name = 'mondo-base.obo'

        # Get latest version
        mondo_dag.get_last_version_from_url(url, f'/download/(v?.+)/{file_name}')
        # Upload files to S3 (if new)
        # not used for now but we could maybe update obo-parser to use that file as input instead of downloading the obo file
        mondo_dag.upload_file_if_new(url=f'{url}/download/{mondo_dag.last_version}/{file_name}', file_name='mondo.obo')

        return mondo_dag.serialize()

    dag_data = download()

    normalized_mondo_terms = SparkOperator(
        task_id='normalized_mondo_terms',
        name='etl-import-hpo-terms',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.HPOMain',
        spark_config='config-etl-medium',
        spark_jar=obo_parser_spark_jar(),
        arguments=[
            'https://github.com/monarch-initiative/mondo/releases/download/{{ ti.xcom_pull(task_ids="download_mondo_terms", key="return_value")["last_version"] }}/mondo-base.obo',
            f'cqgc-{env}-app-datalake',
            'public/mondo_terms',
            'False',
            'MONDO:0700096',
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
            'mondo',
            '{{ ti.xcom_pull(task_ids="download_mondo_terms", key="return_value")["last_version"] }}',
            'mondo_terms_template.json',
            'mondo_terms',
            '1900-01-01 00:00:00',
            f'config/{env}.conf',
        ],
    )

    publish_mondo = publish_index.mondo('{{ ti.xcom_pull(task_ids="download_mondo_terms", key="return_value")["last_version"] }}', color('_'), spark_jar(), task_id='publish_mondo')

    params_validate >> dag_data >> should_continue(dag_data) >> normalized_mondo_terms >> index_mondo_terms >> publish_mondo >> update_public_data_info(dag_data)
