from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import should_continue, update_public_data_info, PublicSourceDag


genomes_dag = PublicSourceDag(
    name='1000_genomes',
    display_name="1000 Genomes Project",
    website="https://www.internationalgenome.org/home",
    raw_folder='1000Genomes'
)

with DAG(
    dag_id=genomes_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    schedule=None,
    params=PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
) as dag:

    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)
    def file():
        url = 'http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502'
        file_name = 'ALL.wgs.phase3_shapeit2_mvncall_integrated_v5c.20130502.sites.vcf.gz'

        # Get latest version
        genomes_dag.set_last_version('20130502')
        # Upload file to S3 (if new)
        genomes_dag.upload_file_if_new(url=f'{url}/{file_name}', file_name=file_name, stream=True)

        return genomes_dag.serialize()

    dag_data = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-1000-genomes-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            '1000genomes',
            '--config', config_file,
            '--steps', 'default'
        ]
    )

    dag_data >> should_continue(dag_data) >> table >> update_public_data_info(dag_data)
