from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from lib import config
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue

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
    default_args=PublicSourceDag.default_args,
) as dag:

    gnomad_s3 = S3Hook(config.s3_gnomad)

    @task(task_id="download_joint_files")
    def download_files():
        # Get latest version
        gnomad_dag.set_last_version("4.1")
        # Upload files to S3 (if new)
        keys = gnomad_s3.list_keys(bucket_name=GNOMAD_S3_BUCKET, prefix=f"release/{gnomad_dag.last_version}/vcf/joint/")
        for key in keys:
            if not key.endswith("/"):
                presigned_url = gnomad_s3.generate_presigned_url("get_object", params={"Bucket": GNOMAD_S3_BUCKET, "Key": key})
                gnomad_dag.upload_file_if_new(url=presigned_url, file_name=key, stream=True, save_version=False)

        gnomad_dag.save_version()
        return gnomad_dag.serialize()

    dag_data = download_files()

    table = SparkOperator(
        task_id="table",
        name="etl_import_gnomad_v4_joint",
        k8s_context=K8sContext.ETL,
        spark_class="bio.ferlab.datalake.spark3.publictables.ImportPublicTable",
        spark_config="config-etl-large",
        trigger_rule=TriggerRule.ALWAYS,
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

    dag_data >> should_continue(dag_data) >> table >> update_public_data_info(dag_data)
