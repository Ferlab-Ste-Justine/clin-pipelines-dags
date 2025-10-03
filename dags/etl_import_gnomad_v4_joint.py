from datetime import datetime
import logging

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from lib import config
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue
from lib.slack import Slack

GNOMAD_S3_BUCKET = "gnomad-public-us-east-1"

gnomad_dag = PublicSourceDag(
    name='gnomad_v4_joint',
    display_name="gnomAD Joint Frequency",
    website="https://gnomad.broadinstitute.org/",
    raw_folder="gnomad_v4"
)

with DAG(
    dag_id=gnomad_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    schedule=None,
    max_active_tasks=1,
    params=PublicSourceDag.params,
    default_args=PublicSourceDag.default_args
) as dag:

    gnomad_s3 = S3Hook(config.s3_gnomad)

    @task(task_id='get_version', on_execute_callback=Slack.notify_dag_start)
    def get_version():
        # Set version
        gnomad_dag.set_last_version("4.1")
        return gnomad_dag
    
    dag_data = get_version()

    @task_group(group_id='download_files')
    def download_files(dag_data: PublicSourceDag):
        @task(task_id="get_filenames")
        def get_filenames(dag_data: PublicSourceDag):
            keys = gnomad_s3.list_keys(bucket_name=GNOMAD_S3_BUCKET, prefix=f"release/{dag_data.last_version}/vcf/joint/")
            file_names = [k for k in keys if not k.endswith("/")]
            if not file_names:
                raise Exception(f"No files found in gnomAD S3 bucket {GNOMAD_S3_BUCKET} for version {dag_data.last_version}")
            logging.info(f"Files found in gnomAD S3: {file_names}")
            return file_names

        @task(task_id="download_file", trigger_rule=TriggerRule.NONE_FAILED)
        def download_file(dag_data: PublicSourceDag, file_name):
            # Upload files to S3 (if new)
            presigned_url = gnomad_s3.generate_presigned_url("get_object", params={"Bucket": GNOMAD_S3_BUCKET, "Key": file_name})
            dag_data.upload_file_if_new(url=presigned_url, file_name=file_name, stream=True, save_version=False)
        
        @task(task_id='save_version')
        def save_version(dag_data: PublicSourceDag):
            dag_data.save_version()

        download_file.partial(dag_data=dag_data).expand(file_name=get_filenames(dag_data)) >> save_version(dag_data)

    table = SparkOperator(
        task_id="table",
        name="etl_import_gnomad_v4_joint",
        k8s_context=K8sContext.ETL,
        spark_class="bio.ferlab.datalake.spark3.publictables.ImportPublicTable",
        spark_config="config-etl-large",
        arguments=[
            "gnomadv4",
            "--config",
            config_file,
            "--steps",
            "default",
            "--app-name",
            "etl_import_gnomad_v4_joint",
        ],
    )

    dag_data >> should_continue(dag_data) >> download_files(dag_data) >> table >> update_public_data_info(dag_data)
