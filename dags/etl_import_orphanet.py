from datetime import datetime
import re

from airflow import DAG
from airflow.decorators import task
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue
from lib.utils import http_get_file


orphanet_dag = PublicSourceDag(
    name='orphanet',
    website="https://www.orphadata.com/",
    schedule='15 6 * * 6#1',  # every first Saturday of the month at 6:15am
)

with DAG(
    dag_id=orphanet_dag.dag_id,
    start_date=datetime(2025, 9, 6),
    schedule=orphanet_dag.schedule,
    params=PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    catchup=False,
    max_active_runs=1
) as dag:

    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)
    def file():
        url = 'https://www.orphadata.com/data/xml'
        genes_file = 'en_product6.xml'
        diseases_file = 'en_product9_ages.xml'

        # Download gene file to check version
        http_get_file(f'{url}/{genes_file}', genes_file, None)
        # Read file to get version
        with open(genes_file, 'r') as f:
            lines = f.readlines()
            orphanet_dag.set_last_version(re.search(r'version="([^"]+)"', lines[1].strip()).group(1))

        # Upload files to S3 (if new)
        orphanet_dag.save_file(genes_file, save_version=False)
        orphanet_dag.upload_file_if_new(url=f'{url}/{diseases_file}', file_name=diseases_file)

        return orphanet_dag.serialize()

    dag_data = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-orphanet-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'orphanet',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_orphanet_table',
        ],
    )

    trigger_genes = TriggerDagRunOperator(
        task_id='genes',
        trigger_dag_id='etl_import_genes',
        wait_for_completion=False,
    )

    dag_data >> should_continue(dag_data) >> table >> trigger_genes >> update_public_data_info(dag_data)
