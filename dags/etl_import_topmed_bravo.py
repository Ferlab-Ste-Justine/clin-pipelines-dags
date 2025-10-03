from datetime import datetime
import logging

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue
from lib.utils import http_get

topmed_dag = PublicSourceDag(
    name='topmed_bravo',
    display_name="BRAVO",
    website="https://legacy.bravo.sph.umich.edu/freeze8/hg38/about",
    raw_folder='topmed'
)

with DAG(
    dag_id=topmed_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    params= {
        'credential': Param('', type=['null', 'string'], description='A TOPMed Bravo valid cookie'),
    } | PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    max_active_tasks=8
) as dag:

    download_link_url = 'https://api.bravo.sph.umich.edu/ui/link?chrom=chr'
    file_prefix = 'bravo-dbsnp-chr'
    file_ext = '.vcf.gz'

    @task(task_id='get_version', on_execute_callback=Slack.notify_dag_start)
    def get_version():
        # Version is hard coded because it can't be found programmatically (the website is JS generated)
        topmed_dag.set_last_version("10")
        return topmed_dag
    
    dag_data = get_version()

    @task_group(group_id='download_files')
    def download_files(dag_data: PublicSourceDag):
        @task
        def download(dag_data: PublicSourceDag, chromosome: str, **context):
            if not dag_data.check_is_new_version():
                logging.info('Skipping download because there is no new version')
                return
            
            cookie = context["params"]["credential"]
            if not cookie:
                raise AirflowFailException('No TOPMed Bravo credentials provided (should be a valid cookie)')

            # Get file url
            download_url = http_get(f'{download_link_url}{chromosome}', headers={'Cookie': cookie}).json()['url']
            # Upload file directly to S3 (if new)
            dag_data.upload_file_if_new(download_url, f'{file_prefix}{chromosome}{file_ext}', headers={'Cookie': cookie}, stream=True, save_version=False)

        @task(task_id='save_version')
        def save_version(dag_data: PublicSourceDag):
            dag_data.save_version()

        download.partial(dag_data=dag_data).expand(chromosome=list(range(1, 23)) + list('X')) >> save_version(dag_data)


    table = SparkOperator(
        task_id='table',
        name='etl-import-topmed-bravo-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'topmed_bravo',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_topmed_bravo_table'
        ],
        on_success_callback=Slack.notify_dag_completion,
    )


    dag_data >> should_continue(dag_data) >> download_files(dag_data) >> table >> update_public_data_info(dag_data)
