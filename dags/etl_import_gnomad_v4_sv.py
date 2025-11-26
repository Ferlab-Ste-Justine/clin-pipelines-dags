from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, should_continue
from lib.utils_etl import spark_jar


gnomad_dag = PublicSourceDag(
    name='gnomad_v4_sv',
    website="https://gnomad.broadinstitute.org/data#v4-structural-variants",
    raw_folder="gnomad_v4",
    add_to_file=False,
)

with DAG(
    dag_id=gnomad_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    schedule=None,
    params={
        'spark_jar': Param('', type=['null', 'string']),
    } | PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    max_active_tasks=1,
    max_active_runs=1
) as dag:

    @task(task_id="file", on_success_callback=Slack.notify_dag_start)
    def file_task():
        # Get latest version
        gnomad_dag.set_last_version("4.1")
        # Upload file to S3
        url = f'https://storage.googleapis.com/gcp-public-data--gnomad/release/{gnomad_dag.last_version}/genome_sv'
        file_name = f'gnomad.v{gnomad_dag.last_version}.sv.sites.vcf.gz'
        gnomad_dag.upload_file_if_new(url=f'{url}/{file_name}', file_name=f'release/{gnomad_dag.last_version}/genome_sv/{file_name}', stream=True)

        return gnomad_dag

    dag_data = file_task()

    table = SparkOperator(
        task_id='table',
        name='etl_import_gnomad_v4_sv',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-small',
        spark_jar=spark_jar(),
        arguments=[
            'gnomadv4sv',
            '--config',
            config_file,
            '--steps',
            'default',
            '--app-name',
            'etl_import_gnomad_v4_sv',
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    dag_data >> should_continue(dag_data) >> table
