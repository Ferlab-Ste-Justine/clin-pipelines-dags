from datetime import datetime

from airflow import DAG
from airflow.models.param import Param


from lib import config
from lib.config import env, Env
from lib.slack import Slack

from lib.tasks.nextflow import post_processing

DEFAULT_INPUT_FILE = f"s3://cqgc-{env}-app-files-import/test_ferlab_post_processing_pipeline/datasets/dragen_4_2_4_small/samplesheet.csv"
DEFAULT_OUTPUT_DIR = f"s3://cqgc-{env}-app-files-scratch/test_ferlab_post_processing_pipeline/dragen_4_2_4_small/output"

if config.show_test_dags and env == Env.QA:
    with DAG(
            dag_id="test_nextflow_post_processing",
            start_date=datetime(2022, 1, 1),
            schedule=None,
            description="DAG for testing the nextflow post processing task",
            params={
                'input': Param(
                    DEFAULT_INPUT_FILE,
                    type='string',
                    description='The input samplesheet file to process.'
                ),
                'outdir': Param(
                    DEFAULT_OUTPUT_DIR,
                    type='string',
                    description='The output directory to store the results.'
                ),
            },
            render_template_as_native_obj=True
    ) as dag:
        post_processing.run(
            input="{{ params.input }}",
            outdir="{{ params.outdir }}",
            on_execute_callback=Slack.notify_dag_start,
            on_success_callback=Slack.notify_dag_completion
        )
