from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, update_public_data_entry_task, should_continue
from lib.utils import http_get

topmed_dag = PublicSourceDag(
    name='topmed_bravo',
    display_name="BRAVO",
    website="https://legacy.bravo.sph.umich.edu/freeze8/hg38/about",
    raw_folder='topmed',
    schedule='0 7 * */3 6'  # every 3 months on the first Saturday at 7am
)

with DAG(
    dag_id=topmed_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    schedule=topmed_dag.schedule,
    catchup=False,
    params= {
        'freeze_version': Param(None, type=['null', 'integer']),
        'credential': Param('', type=['null', 'string']),
    } | PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    max_active_tasks=8
) as dag:

    download_link_url = 'https://api.bravo.sph.umich.edu/ui/link?chrom=chr'
    file_prefix = 'bravo-dbsnp-chr'
    file_ext = '.vcf.gz'
    chromosomes = list(range(1, 23)) + list('X')

    with TaskGroup(group_id='files') as files:

        @task(task_id='init', on_execute_callback=Slack.notify_dag_start)
        def init(**context):
            # Get latest version
            topmed_dag.set_last_version(str(context["params"]["freeze_version"]))
            topmed_dag.check_is_new_version()
            return topmed_dag.serialize()

        dag_data = init()

        @task
        def download(dag_data, chromosome: str, **context):
            dag_source = PublicSourceDag.deserialize(dag_data)

            cookie = context["params"]["credential"]
            if not cookie:
                raise AirflowFailException('No TOPMed Bravo credentials provided (should be a valid cookie)')

            # Get file url
            download_url = http_get(f'{download_link_url}{chromosome}', headers={'Cookie': cookie}).json()['url']
            # Upload file directly to S3 (if new)
            dag_source.upload_file_if_new(download_url, f'{file_prefix}{chromosome}{file_ext}', headers={'Cookie': cookie}, stream=True, save_version=False)

        variants = download.partial(dag_data=dag_data).expand(chromosome=chromosomes)

        @task(task_id='release')
        def release(dag_data):
            dag_source = PublicSourceDag.deserialize(dag_data)
            dag_source.save_version()

        dag_data >> should_continue(dag_data) >> variants >> release(dag_data)


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


    files >> table >> update_public_data_entry_task(dag_data)
