from airflow import DAG
from datetime import datetime
from lib.config import env, K8sContext
from lib.operators.spark import SparkOperator
from lib.slack import Slack


with DAG(
    dag_id='etl_reset_enrich_consequences',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    reset_enrich_consequences = SparkOperator(
        task_id='reset_enrich_consequences',
        name='etl-reset-enrich-consequences',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
        spark_config='enriched-etl',
        arguments=[
            f'config/{env}.conf', 'initial', 'consequences',
        ],
        on_execute_callback=Slack.notify_dag_start,
        on_success_callback=Slack.notify_dag_complete,
    )
