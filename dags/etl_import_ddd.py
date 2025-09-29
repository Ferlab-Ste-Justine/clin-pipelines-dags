import re
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue
from lib.utils import get_md5_from_url
from lib.utils import http_get


ddd_dag = PublicSourceDag(
    name='ddd',
    display_name="Gene2Phenotype",
    website="https://www.ebi.ac.uk/gene2phenotype/",
)

with DAG(
    dag_id=ddd_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    schedule=None,
    params=PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    catchup=False,
    max_active_runs=1
) as dag:

    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)  
    def file():
        url = 'https://ftp.ebi.ac.uk/pub/databases/gene2phenotype/G2P_data_downloads'

        # Get latest version
        versions = re.findall('href="(\d{4}_\d{2}_\d{2})/"', http_get(url).text)
        ddd_dag.set_last_version(sorted(versions, reverse=True)[0])

        file_name = f'DDG2P_{ddd_dag.last_version.replace("_", "-")}.csv.gz'
        md5_file_name = f'{file_name}.md5'

        # Get MD5
        md5 = get_md5_from_url(f'{url}/{ddd_dag.last_version}/{md5_file_name}')

        # Upload file to S3 (if new)
        ddd_dag.upload_file_if_new(url=f'{url}/{ddd_dag.last_version}/{file_name}', file_name='DDG2P.csv.gz', md5_hash=md5['hash'])

        return ddd_dag.serialize()

    dag_data = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-ddd-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'ddd',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_ddd_table',
        ]
    )

    trigger_genes = TriggerDagRunOperator(
        task_id='genes',
        trigger_dag_id='etl_import_genes',
        wait_for_completion=False,
    )

    dag_data >> should_continue(dag_data) >> table >> trigger_genes >> update_public_data_info(dag_data)
