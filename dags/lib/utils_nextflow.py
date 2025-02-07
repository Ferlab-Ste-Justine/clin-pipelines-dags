from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config


@task.short_circuit(ignore_downstream_trigger_rules=False)
def check_input_file_exists(bucket: str, key: str, skip: str):
    """
    Check if the input file exists in the specified bucket. If the file is missing, the next task will be skipped.
    """
    if skip:
        raise AirflowSkipException()

    s3 = S3Hook(config.s3_conn_id)
    return s3.check_for_key(key, bucket)
