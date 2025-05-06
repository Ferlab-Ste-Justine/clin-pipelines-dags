from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from lib import config
from lib.config import env, Env
from lib.slack import Slack

from lib.tasks.nextflow import cnv_post_processing

DEFAULT_INPUT_FILE = f"s3://cqgc-{env}-app-files-import/test_analysis/clin-4404-cnv-post-processing-test-dag/datasets/dragen_4_2_4_small/samplesheet.csv"
DEFAULT_JOB_HASH = "test_dragen_4_2_4_small"

if config.show_test_dags and env == Env.QA:
    with DAG(
            dag_id="test_nextflow_cnv_post_processing",
            start_date=datetime(2022, 1, 1),
            schedule=None,
            description="DAG for testing the nextflow cnv post processing task",
            params={
                'input': Param(
                    DEFAULT_INPUT_FILE,
                    type='string',
                    description='The input samplesheet file to process.'
                ),
                'job_hash': Param(
                    DEFAULT_JOB_HASH,
                    type='string',
                    description='Unique identifier for the job. Will be used to name the output directory.'
                ),
            },
            render_template_as_native_obj=True
    ) as dag:
        cnv_post_processing.run(
            input="{{ params.input }}",
            job_hash="{{ params.job_hash }}",
            on_execute_callback=Slack.notify_dag_start,
            on_success_callback=Slack.notify_dag_completion
        )
