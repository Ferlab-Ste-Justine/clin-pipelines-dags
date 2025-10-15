from datetime import datetime
import re

from airflow import DAG
from airflow.decorators import task

from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue
from lib.utils import http_get


refseq_annotation_dag = PublicSourceDag(
    name='refseq_annotation',
    display_name="NCBI RefSeq",
    website="https://www.ncbi.nlm.nih.gov/refseq/",
    schedule='15 8 * * 6',  # every Saturday at 8:15am
    raw_folder='refseq'
)

with DAG(
    dag_id=refseq_annotation_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    schedule=refseq_annotation_dag.schedule,
    catchup=False,
    params=PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
) as dag:

    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)
    def file():
        url = 'https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/annotation_releases/current'

        # Get latest version
        match = re.search('GCF_000001405\.([^/]+)/', http_get(url).text)
        last_folder = match.group()
        refseq_annotation_dag.set_last_version(match.group(1))
        file_name = re.search('>(GCF_000001405.+gff.gz)', http_get(f'{url}/{last_folder}').text).group(1)
        # Upload files to S3 (if new)
        refseq_annotation_dag.upload_file_if_new(f'{url}/{last_folder}{file_name}', 'GCF_GRCh38_genomic.gff.gz')

        return refseq_annotation_dag

    dag_data = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-refseq-annotation-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'refseq_annotation',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_refseq_annotation_table',
        ]
    )

    dag_data >> should_continue(dag_data) >> table >> update_public_data_info(dag_data)
