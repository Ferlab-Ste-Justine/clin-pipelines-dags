from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator
from lib.slack import Slack

with DAG(
        dag_id='etl_import_clinvar_vep',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        max_active_tasks=1,  # Only one task can be scheduled at a time
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
) as dag:

    rare_variant_table = SparkOperator(
        task_id='clinvar_vep_table',
        name='etl-import-clinvar-vep-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='enriched-etl',
        arguments=[f'config/{env}.conf', 'default', 'clinvar_vep'],
        on_execute_callback=Slack.notify_dag_start
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    rare_variant_table >> slack
