from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue


ensembl_dag = PublicSourceDag(
    name='ensembl',
    display_name="Ensembl",
    website="https://www.ensembl.org/info/data/index.html"
)

with DAG(
    dag_id=ensembl_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    schedule=ensembl_dag.schedule,
    params={
        "version": Param(None, type=['null', 'integer'], description="Version to use. If not set, the latest version will be used."),
    } | PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    catchup=False,
) as dag:

    ftp_url='https://ftp.ensembl.org/pub'

    @task(task_id='get_version', on_execute_callback=Slack.notify_dag_start)
    def get_version(**context):
        param_version = context["params"]["version"]
        if param_version:
            ensembl_dag.set_last_version(str(param_version))
        else:
            ensembl_dag.set_last_version_from_url(f'{ftp_url}/VERSION', r'(\d+)')
        return ensembl_dag

    dag_data = get_version()

    @task_group(group_id='download_files')
    def download_files(dag_data: PublicSourceDag):        
        @task(task_id='download_file')
        def download_file(dag_data: PublicSourceDag, type: str):
            file_name_base = 'Homo_sapiens.GRCh38'
            dag_data.upload_file_if_new(
                url=f'{ftp_url}/release-{dag_data.last_version}/tsv/homo_sapiens/{file_name_base}.{dag_data.last_version}.{type}.tsv.gz',
                file_name=f'{file_name_base}.{type}.tsv.gz',
                save_version=False
            )

        @task(task_id='save_version')
        def save_version(dag_data: PublicSourceDag):
            dag_data.save_version()

        download_file.partial(dag_data=dag_data).expand(type=['ena', 'entrez', 'refseq', 'uniprot']) >> save_version(dag_data)

    table = SparkOperator(
        task_id='table',
        name='etl-import-ensembl-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'ensembl_mapping',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_ensembl_table',
        ]
    )

    dag_data >> should_continue(dag_data) >> download_files(dag_data) >> table >> update_public_data_info(dag_data)
