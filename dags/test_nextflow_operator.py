from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from lib import config
from lib.config import env, Env, K8sContext
from lib.operators.nextflow import NextflowOperator
from lib.slack import Slack

DEFAULT_NEXTFLOW_COMMAND = [
    "nextflow",
    "run",
    "nextflow-io/hello",
    "-c",
    "/root/nextflow/config/nextflow.config"
]
if (config.show_test_dags or env in [Env.QA, Env.STAGING]):
    with DAG(
        dag_id="test_nextflow_operator",
        start_date=datetime(2022, 1, 1),
        schedule=None,
        description="Run an ad-hoc nextflow command in a kubernetes pod.",
        params={
            "nextflow_command": Param(
                DEFAULT_NEXTFLOW_COMMAND,
                type="array",
                description=f"""The nextflow command to be executed.
                This should be expressed as a list of arguments, which will
                be passed to the bash shell in the nextflow pod. Here each
                line represent a separate argument.

                Here the `/root/nextflow/config/nextflow.config` file is a
                valid nextflow configuration file that will be available in
                the pod. It contains configuration settings specific to the
                Qlin execution environment. You should always include it in
                your nextflow command, unless you have a valid reason not to.

                The nextflow working directory is set to
                `{config.nextflow_working_dir}`. You can overwrite it via the
                `-work-dir` option, but please use a location under this
                directory.
                """
            )
        },
        render_template_as_native_obj=True
    ) as dag:

        NextflowOperator(
            task_id='test_nextflow_operator',
            name="test_nextflow_operator",  # prefix for the pod name
            k8s_context=K8sContext.ETL,
            arguments="{{ params.nextflow_command }}",
            on_execute_callback=Slack.notify_dag_start,
            on_success_callback=Slack.notify_dag_completion,
        )
