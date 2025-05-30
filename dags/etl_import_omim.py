from datetime import datetime

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack

with DAG(
    dag_id='etl_import_omim',
    start_date=datetime(2022, 1, 1),
    schedule=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    table = SparkOperator(
        task_id='table',
        name='etl-import-omim-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'omim',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_omim_table',
        ],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_execute_callback=Slack.notify_dag_start,
        on_success_callback=Slack.notify_dag_completion,
    )
