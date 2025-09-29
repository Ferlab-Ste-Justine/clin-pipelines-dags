from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue
from lib.utils import http_get


ensembl_dag = PublicSourceDag(
    name='ensembl',
    display_name="Ensembl",
    website="https://www.ensembl.org/info/data/index.html",
    schedule='45 7 * * 6',  # every Saturday at 7:45am
)

with DAG(
    dag_id=ensembl_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    schedule=ensembl_dag.schedule,
    params=PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    catchup=False,
) as dag:

    file_name_base = 'Homo_sapiens.GRCh38'
    url = 'http://ftp.ensembl.org/pub/current_tsv/homo_sapiens'
    types = ['ena', 'entrez', 'refseq', 'uniprot']

    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)
    def files():
        # Get latest version
        ensembl_dag.set_last_version(http_get('https://ftp.ensembl.org/pub/VERSION').text.strip())
        # Upload files to S3 (if new)
        for type in types:
            ensembl_dag.upload_file_if_new(
                url=f'{url}/{file_name_base}.{ensembl_dag.last_version}.{type}.tsv.gz',
                file_name=f'{file_name_base}.{type}.tsv.gz',
                save_version=False
            )

        ensembl_dag.save_version()
        return ensembl_dag.serialize()


    dag_data = files()

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
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    dag_data >> should_continue(dag_data) >> table >> update_public_data_info(dag_data)
