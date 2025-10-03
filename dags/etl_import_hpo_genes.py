import re
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue
from lib.utils import http_get
from lib.utils_etl import spark_jar

hpo_genes_dag = PublicSourceDag(
    name='hpo_genes',
    display_name="Human Phenotype Ontology (HPO)",
    website="https://hpo.jax.org/",
    raw_folder='hpo',
    schedule='30 7 * * 6',  # every Saturday at 7:30am
)

with DAG(
    dag_id=hpo_genes_dag.dag_id,
    start_date=datetime(2022, 8, 16),
    schedule=hpo_genes_dag.schedule,
    params={
        'spark_jar': Param('', type=['null', 'string'])
    } | PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1
) as dag:

    @task(task_id='download_hpo_genes')
    def download():
        url = 'https://github.com/obophenotype/human-phenotype-ontology/releases'
        file_name = 'genes_to_phenotype.txt'

        # Get latest version
        hpo_genes_dag.set_last_version(re.search(f'/download/(v?.+)/{file_name}', http_get(url).text).group(1))
        # Upload files to S3 (if new)
        hpo_genes_dag.upload_file_if_new(url=f'{url}/download/{hpo_genes_dag.last_version}/{file_name}', file_name=file_name)

        return hpo_genes_dag

    dag_data = download()

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

    dag_data >> should_continue(dag_data) >> normalized_hpo_genes >> trigger_genes >> update_public_data_info(dag_data)
