from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from lib import config
from lib.config import env, Env, nextflow_base_config
from lib.config_nextflow import default_nextflow_config_file
from lib.slack import Slack

DEFAULT_NEXTFLOW_PIPELINE = "nextflow-io/hello"

if config.show_test_dags or env in [Env.QA, Env.STAGING]:
    with DAG(
            dag_id="test_nextflow_operator",
            start_date=datetime(2022, 1, 1),
            schedule=None,
            description="Run an ad-hoc nextflow command in a kubernetes pod.",
            params={
                'nextflow_pipeline': Param(DEFAULT_NEXTFLOW_PIPELINE, type='string'),
                'nextflow_extra_config_files': Param(
                    [], type=['null', 'array'], description=f"""Additional config files to use alongside the 
                    default config file `{default_nextflow_config_file}`. This file is a valid nextflow configuration 
                    that contains configuration settings specific to the Qlin execution environment."""),
                'nextflow_arguments': Param(
                    [], type=['null', 'array'], description=f"""The nextflow command to be executed. This should 
                    be expressed as a list of arguments, which will be passed to the bash shell in the nextflow pod. 
                    Here each line represents a distinct argument.
                    
                    The nextflow working directory is set to `{nextflow_base_config.nextflow_working_dir}`. You can
                    overwrite it via the `-work-dir` option, but please use a location under this directory.""")
            },
            render_template_as_native_obj=True
    ) as dag:
        def nextflow_extra_config_files() -> str:
            return '{{ params.nextflow_extra_config_files or "" }}'


        def nextflow_arguments() -> str:
            return '{{ params.nextflow_arguments or "" }}'


        nextflow_base_config \
            .with_pipeline('{{ params.nextflow_pipeline }}') \
            .append_config_files(nextflow_extra_config_files()) \
            .append_args(nextflow_arguments()) \
            .operator(
                task_id='test_nextflow_operator',
                name="test_nextflow_operator",
                on_execute_callback=Slack.notify_dag_start,
                on_success_callback=Slack.notify_dag_completion,
            )
