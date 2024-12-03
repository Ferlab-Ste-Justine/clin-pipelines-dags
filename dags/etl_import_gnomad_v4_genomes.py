import logging
from datetime import datetime
from enum import Enum

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib.config import K8sContext, config_file
from lib.slack import Slack
from lib.operators.spark import SparkOperator


class SequencingType(Enum):
    GENOMES = "genomes"
    EXOMES = "exomes"


LATEST_VERSION = "4.1"
GNOMAD_S3_BUCKET = "gnomad-public-us-east-1"


@dag(
    dag_id="etl_import_gnomad_v4_genomes",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    max_active_tasks=1,
    default_args={
        "on_failure_callback": Slack.notify_task_failure,
    },
)
def etl_import_gnomad_v4_genomes():

    table = SparkOperator(
        task_id='table',
        name='etl_import_gnomad_v4_genomes',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'gnomadv4',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_gnomad_v4_genomes',
        ],
    )

    slack = EmptyOperator(task_id="slack", on_success_callback=Slack.notify_dag_completion)

    table >> slack


etl_import_gnomad_v4_genomes()
