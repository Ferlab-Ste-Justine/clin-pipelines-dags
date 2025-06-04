import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from lib import config
from lib.config import clin_datalake_bucket, K8sContext, config_file
from lib.slack import Slack
from lib.operators.spark import SparkOperator
from lib.utils_s3 import stream_upload_to_s3, get_s3_file_version


LATEST_VERSION = "4.1"
GNOMAD_S3_BUCKET = "gnomad-public-us-east-1"


@dag(
    dag_id="etl_import_gnomad_v4_joint",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    max_active_tasks=1,
    default_args={
        "on_failure_callback": Slack.notify_task_failure,
    },
)
def etl_import_gnomad_v4_joint():

    clin_s3 = S3Hook(config.s3_conn_id)
    gnomad_s3 = S3Hook(config.s3_gnomad)

    @task(task_id="download_joint_files")
    def download_files():
        destination_prefix = f'raw/landing/gnomad_v4'
        gnomad_prefix = f"release/{LATEST_VERSION}/vcf/joint"

        # Get imported version
        imported_ver = get_s3_file_version(clin_s3, clin_datalake_bucket, destination_prefix)
        logging.info(f"Current gnomAD joint imported version: {imported_ver}")

        # Skip task if up to date
        if imported_ver == LATEST_VERSION:
            logging.warning(f"Skipping import of gnomAD joint. Imported version {imported_ver} is up to date.")
            raise AirflowSkipException()

        # Download files
        logging.info(f"Importing gnomAD joint version: {LATEST_VERSION}")
        keys = keys = gnomad_s3.list_keys(bucket_name=GNOMAD_S3_BUCKET, prefix=f"{gnomad_prefix}/")

        for key in keys:
            if not key.endswith("/"):
                generation_params = {"Bucket": GNOMAD_S3_BUCKET, "Key": key}
                presigned_url = gnomad_s3.generate_presigned_url("get_object", params=generation_params)
                destination_key = f"{destination_prefix}/{key}"

                logging.info(f"Importing file {key}")

                stream_upload_to_s3(clin_s3, clin_datalake_bucket, destination_key, presigned_url)

        # Update version
        logging.info(f"Version {LATEST_VERSION} of gnomAD joint imported to S3.")
        clin_s3.load_string(LATEST_VERSION, f"{destination_prefix}.version", clin_datalake_bucket, replace=True)

    files = download_files()

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

    slack = EmptyOperator(task_id="slack", on_success_callback=Slack.notify_dag_completion)

    files >> table >> slack


etl_import_gnomad_v4_joint()
