from datetime import datetime
from typing import List, Set

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.datasets import enriched_clinical
from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.groups.nextflow.nextflow_germline import nextflow_germline
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks import batch_type
from lib.tasks.clinical import get_all_analysis_ids
from lib.tasks.params import get_sequencing_ids
from lib.tasks.params_validate import validate_color
from lib.utils_etl import (
    ClinAnalysis, color,
    get_germline_analysis_ids, get_ingest_dag_configs_by_analysis_ids,
    spark_jar)
from pandas import DataFrame

with DAG(
        dag_id='etl_run',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        catchup=False,
        params={
            'sequencing_ids': Param([], type=['null', 'array']),
            'color': Param('', type=['null', 'string']),
            'spark_jar': Param('', type=['null', 'string']),
        },
        render_template_as_native_obj=True,
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_tasks=1,
        max_active_runs=1
) as dag:

    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )

    # Disabling callback as the start task already perform the slack notification
    params_validate = validate_color.override(on_execute_callback=None)(color=color())

    ingest_fhir_group = ingest_fhir(
        batch_ids=[],  # No associated "batch"
        color=color(),
        skip_all=False,
        skip_import=True,  # Skipping because the data is already imported via the prescription API
        skip_batch=False,
        spark_jar=spark_jar()
    )

    @task.virtualenv(task_id='get_all_sequencing_ids', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical], max_active_tis_per_dag=1)
    def get_all_sequencing_ids(sequencing_ids: List[str]) -> Set[str]:
        """
        Retrieves all sequencing IDs that share the same analysis ID as the given sequencing IDs from the
        `enriched_clinical` Delta table. Validates that all sequencing IDs have at least one associated SNV VCF file and
        that all proband sequencing IDs have at least one associated clinical sign.
        """
        import logging

        from airflow.exceptions import AirflowFailException
        from lib.datasets import enriched_clinical
        from lib.utils_etl_tables import get_analysis_ids, to_pandas

        distinct_sequencing_ids: Set[str] = set(sequencing_ids)
        logging.info(f"Distinct input sequencing IDs: {distinct_sequencing_ids}")

        df: DataFrame = to_pandas(enriched_clinical.uri)
        clinical_df = df[["sequencing_id", "analysis_id", "is_proband", "clinical_signs", "snv_vcf_germline_urls", "batch_id"]]

        # Get all sequencing IDs that share the same analysis ID as the given sequencing IDs
        analysis_ids = get_analysis_ids(clinical_df, sequencing_ids=distinct_sequencing_ids)
        _all_sequencing_ids = set(clinical_df.loc[clinical_df["analysis_id"].isin(analysis_ids), "sequencing_id"])
        if not _all_sequencing_ids:
            raise AirflowFailException("No sequencing IDs found for the given input sequencing IDs")
        logging.info(f"Analysis IDs associated with input sequencing IDs: {analysis_ids}")
        logging.info(f"All sequencing IDs associated with input sequencing IDs: {_all_sequencing_ids}")

        return _all_sequencing_ids

    get_sequencing_ids_task = get_sequencing_ids()
    get_all_sequencing_ids_task = get_all_sequencing_ids(get_sequencing_ids_task)
    get_all_analysis_ids_task = get_all_analysis_ids(sequencing_ids=get_sequencing_ids_task)

    detect_batch_types_task = batch_type.detect(analysis_ids=get_all_analysis_ids_task, allowMultipleIdentifierTypes=True)

    get_germline_analysis_ids_task = get_germline_analysis_ids(all_batch_types=detect_batch_types_task, analysis_ids=get_all_analysis_ids_task)
    nextflow_germline_task_group = nextflow_germline(analysis_ids=get_germline_analysis_ids_task)

    get_ingest_dag_configs_by_analysis_ids_task = get_ingest_dag_configs_by_analysis_ids.partial(all_batch_types=detect_batch_types_task, analysis_ids=get_all_analysis_ids_task).expand(analysisType=[ClinAnalysis.GERMLINE.value, ClinAnalysis.SOMATIC_TUMOR_ONLY.value])

    trigger_ingest_by_sequencing_ids_dags = TriggerDagRunOperator.partial(
        task_id='ingest_sequencing_ids',
        trigger_dag_id='etl_ingest',
        wait_for_completion=True,
    ).expand(conf=get_ingest_dag_configs_by_analysis_ids_task)

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    (
        start >> params_validate >>
        ingest_fhir_group >>
        get_sequencing_ids_task >> get_all_sequencing_ids_task >> get_all_analysis_ids_task >>
        detect_batch_types_task >>
        get_germline_analysis_ids_task >> nextflow_germline_task_group >>
        get_ingest_dag_configs_by_analysis_ids_task >> trigger_ingest_by_sequencing_ids_dags >>
        slack
    )
