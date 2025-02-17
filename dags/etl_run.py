import logging
from datetime import datetime
from typing import List, Set

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from pandas import DataFrame

from lib.datasets import enriched_clinical
from lib.slack import Slack

with DAG(
    dag_id='etl_run',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    params={
        'sequencing_ids': Param([], type=['null', 'array']),
    },
    render_template_as_native_obj=True,
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
    max_active_tasks=1,
    max_active_runs=1
) as dag:

    def sequencing_ids():
        return '{{ params.sequencing_ids }}'

    def _run(sequencing_ids: List[str]):
        logging.info(f'Run ETLs for total: {len(sequencing_ids)} sequencing_ids: {sequencing_ids}')

    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )


    @task.virtualenv(task_id='get_all_sequencing_ids', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
    def get_all_sequencing_ids(_sequencing_ids: List[str]) -> Set[str]:
        """
        Retrieves all sequencing IDs that share the same analysis ID as the given sequencing IDs from the
        `enriched_clinical` Delta table.

        TODO:
            - Rename service_request_id to sequencing_id when enriched_clinical table is refactored
            - Rename analysis_service_request_id to analysis_id when enriched_clinical table is refactored
        """
        import json

        from airflow.hooks.base import BaseHook
        from deltalake import DeltaTable
        from lib.config import s3_conn_id
        from lib.datasets import enriched_clinical

        distinct_sequencing_ids: Set[str] = set(_sequencing_ids)

        conn = BaseHook.get_connection(s3_conn_id)
        host = json.loads(conn.get_extra()).get("host")
        storage_options = {
            "AWS_ACCESS_KEY_ID": conn.login,
            "AWS_SECRET_ACCESS_KEY": conn.get_password(),
            "AWS_ENDPOINT_URL": host
        }

        dt: DeltaTable = DeltaTable(enriched_clinical.uri, storage_options=storage_options)
        df: DataFrame = dt.to_pandas()
        filtered_df = df[["service_request_id", "analysis_service_request_id"]]
        analysis_ids = set(filtered_df.loc[filtered_df["service_request_id"].isin(distinct_sequencing_ids), "analysis_service_request_id"])
        all_sequencing_ids = set(filtered_df.loc[filtered_df["analysis_service_request_id"].isin(analysis_ids), "service_request_id"])

        return all_sequencing_ids


    run_etl = PythonOperator(
        task_id='run',
        op_args=[sequencing_ids()],
        python_callable=_run,
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> run_etl >> get_all_sequencing_ids(sequencing_ids()) >> slack
