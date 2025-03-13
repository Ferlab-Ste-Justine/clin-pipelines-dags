import logging
from datetime import datetime
from typing import List, Set

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from pandas import DataFrame

from lib.datasets import enriched_clinical
from lib.slack import Slack
from lib.tasks import nextflow

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


    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )


    @task.virtualenv(task_id='get_all_sequencing_ids', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
    def get_all_sequencing_ids(_sequencing_ids: List[str]) -> Set[str]:
        """
        Retrieves all sequencing IDs that share the same analysis ID as the given sequencing IDs from the
        `enriched_clinical` Delta table. Validates that all sequencing IDs have at least one associated SNV VCF file and
        that all proband sequencing IDs have at least one associated clinical sign.

        TODO:
            - Rename service_request_id to sequencing_id when enriched_clinical table is refactored
            - Rename analysis_service_request_id to analysis_id when enriched_clinical table is refactored
        """
        import logging

        from airflow.exceptions import AirflowFailException
        from deltalake import DeltaTable
        from lib.config import s3_conn_id
        from lib.datasets import enriched_clinical
        from lib.utils_s3 import get_s3_storage_options

        distinct_sequencing_ids: Set[str] = set(_sequencing_ids)
        logging.info(f"Distinct input sequencing IDs: {distinct_sequencing_ids}")

        storage_options = get_s3_storage_options(s3_conn_id)
        dt: DeltaTable = DeltaTable(enriched_clinical.uri, storage_options=storage_options)
        df: DataFrame = dt.to_pandas()

        clinical_df = df[["service_request_id", "analysis_service_request_id", "is_proband", "clinical_signs", "snv_vcf_urls"]]

        # Get all sequencing IDs that share the same analysis ID as the given sequencing IDs
        analysis_ids = set(clinical_df.loc[clinical_df["service_request_id"].isin(distinct_sequencing_ids), "analysis_service_request_id"])
        _all_sequencing_ids = set(clinical_df.loc[clinical_df["analysis_service_request_id"].isin(analysis_ids), "service_request_id"])

        if not _all_sequencing_ids:
            raise AirflowFailException("No sequencing IDs found for the given input sequencing IDs")

        logging.info(f"Analysis IDs associated with input sequencing IDs: {analysis_ids}")
        logging.info(f"All sequencing IDs associated with input sequencing IDs: {_all_sequencing_ids}")

        # Filter clinical_df for sequencing IDs
        filtered_df = clinical_df[clinical_df["service_request_id"].isin(_all_sequencing_ids)]

        # Ensure all sequencing IDs have at least one associated snv vcf url
        missing_snv_seq_ids = set(filtered_df.loc[filtered_df["snv_vcf_urls"].isna() |
                                                  (filtered_df["snv_vcf_urls"].str.len() == 0), "service_request_id"])

        if missing_snv_seq_ids:
            raise AirflowFailException(f"Some sequencing IDs don't have associated SNV VCF files: {missing_snv_seq_ids}")

        # Ensure all proband sequencing IDs have at least one associated clinical sign
        missing_clinical_signs_seq_ids = set(filtered_df.loc[(filtered_df["is_proband"]) &
                                                             ((filtered_df["clinical_signs"].isna()) |
                                                              (filtered_df["clinical_signs"].str.len() == 0)), "service_request_id"])

        if missing_clinical_signs_seq_ids:
            raise AirflowFailException(f"Some proband sequencing IDs don't have at least one associated clinical sign: {missing_clinical_signs_seq_ids}")

        return _all_sequencing_ids


    all_sequencing_ids = get_all_sequencing_ids(sequencing_ids())


    @task(task_id="run")
    def run(sequencing_ids: Set[str]):
        logging.info(f'Run ETLs for total: {len(sequencing_ids)} sequencing_ids: {sequencing_ids}')


    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    start >> all_sequencing_ids >> run(all_sequencing_ids) >> slack
