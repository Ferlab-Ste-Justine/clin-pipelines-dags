from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib import config
from lib.config import K8sContext, config_file, env
from lib.operators.spark import SparkOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue
from lib.utils_s3 import download_and_check_md5, get_s3_file_md5


human_genes_dag = PublicSourceDag(
    name='human_genes',
    display_name="NCBI Gene",
    website="https://www.ncbi.nlm.nih.gov/gene"
)

with DAG(
    dag_id=human_genes_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    schedule=None,
    params=PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    catchup=False,
    max_active_runs=1
) as dag:

    @task(on_execute_callback=Slack.notify_dag_start)
    def file():
        url = 'https://ftp.ncbi.nlm.nih.gov/refseq/H_sapiens'
        file_name = 'Homo_sapiens.gene_info.gz'

        # Get latest version
        human_genes_dag.get_last_version_from_url(url, 'Homo_sapiens.gene_info.gz</a> (\d{4}-\d{2}-\d{2})')
        # Upload files to S3 (if new)
        human_genes_dag.upload_file_if_new(url, file_name)

        return human_genes_dag.serialize()
    
    dag_data = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-human-genes-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'refseq_human_genes',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_human_genes_table',
        ]
    )

    trigger_genes = TriggerDagRunOperator(
        task_id='genes',
        trigger_dag_id='etl_import_genes',
        wait_for_completion=False,
    )

    dag_data >> should_continue(dag_data) >> table >> trigger_genes >> update_public_data_info(dag_data)
