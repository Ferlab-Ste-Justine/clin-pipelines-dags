from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from lib.config import env, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, should_continue

GNOMAD_VERSION = "2.1.1"

gnomad_dag = PublicSourceDag(
    name='gnomad_constraint',
    website="https://gnomad.broadinstitute.org/",
    raw_folder=f'gnomad_v{GNOMAD_VERSION.replace(".", "_")}',
    add_to_file=False,
)

with DAG(
        dag_id=gnomad_dag.dag_id,
        start_date=datetime(2022, 1, 1),
        schedule=None,
        params=PublicSourceDag.params,
        default_args=PublicSourceDag.default_args,
) as dag:
    
    @task(task_id="file", on_success_callback=Slack.notify_dag_start)
    def file():
        # Get latest version
        gnomad_dag.set_last_version("2.1.1")
        # Upload file to S3
        url = f'https://gnomad-public-us-east-1.s3.amazonaws.com/release/{gnomad_dag.last_version}/constraint'
        file = f'gnomad.v{gnomad_dag.last_version}.lof_metrics.by_gene.txt.bgz'
        gnomad_dag.upload_file_if_new(url=f'{url}/{file}', file_name=file)

        return gnomad_dag.serialize()

    dag_data = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-gnomad-constraint',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'gnomad_constraint',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_gnomad_constraint',
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    dag_data >> should_continue(dag_data) >> table
