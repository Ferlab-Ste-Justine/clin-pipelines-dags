from datetime import datetime
from typing import List, Set

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.config import K8sContext
from lib.config_nextflow import (nextflow_bucket,
                                 nextflow_post_processing_exomiser_output_key)
from lib.datasets import enriched_clinical
from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.operators.pipeline import PipelineOperator
from lib.slack import Slack
from lib.tasks.nextflow import exomiser, post_processing
from lib.tasks.params_validate import validate_color
from lib.utils_etl import color, spark_jar
from pandas import DataFrame

with DAG(
        dag_id='etl_run',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
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
    def get_sequencing_ids():
        return '{{ params.sequencing_ids }}'

    start = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )

    # Disabling callback as the start task already perform the slack notification
    params_validate = validate_color.override(on_execute_callback=None)(color=color())

    ingest_fhir_group = ingest_fhir(
        batch_id='',  # No associated "batch"
        color=color(),
        skip_all=False,
        skip_import=True,  # Skipping because the data is already imported via the prescription API
        skip_batch=False,
        spark_jar=spark_jar()
    )

    @task.virtualenv(task_id='get_all_sequencing_ids', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
    def get_all_sequencing_ids(sequencing_ids: List[str]) -> Set[str]:
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
        from lib.datasets import enriched_clinical
        from lib.utils_etl_tables import get_analysis_ids, to_pandas

        distinct_sequencing_ids: Set[str] = set(sequencing_ids)
        logging.info(f"Distinct input sequencing IDs: {distinct_sequencing_ids}")

        df: DataFrame = to_pandas(enriched_clinical.uri)
        clinical_df = df[["service_request_id", "analysis_service_request_id", "is_proband", "clinical_signs", "snv_vcf_urls"]]

        # Get all sequencing IDs that share the same analysis ID as the given sequencing IDs
        analysis_ids = get_analysis_ids(clinical_df, distinct_sequencing_ids)
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

    @task.virtualenv(task_id='get_all_analysis_ids', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
    def get_all_analysis_ids(all_sequencing_ids: Set[str]) -> str:
        """
        Retrieves all analysis IDs for every sequencing IDs
        """
        from lib.datasets import enriched_clinical
        from lib.utils_etl_tables import get_analysis_ids, to_pandas

        df: DataFrame = to_pandas(enriched_clinical.uri)
        clinical_df = df[["service_request_id", "analysis_service_request_id", "is_proband", "clinical_signs", "snv_vcf_urls"]]

        return sorted(get_analysis_ids(clinical_df, all_sequencing_ids))

    @task.virtualenv(task_id='get_job_hash', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
    def get_job_hash(all_analysis_ids: Set[str]) -> str:
        """
        Generate a unique hash for the job using the analysis IDs associated to the input sequencing IDs. The hash is used to name the input samplesheet file and the output directory in the nextflow post-processing pipeline.
        """
        from lib.utils import urlsafe_hash

        return urlsafe_hash(all_analysis_ids, length=14)  # 14 is safe for up to 1B hashes
    
    @task
    def prepare_exomiser_references_analysis_ids(all_analysis_ids: Set[str]) -> List[str]:
        return ','.join(all_analysis_ids)

    get_all_sequencing_ids_task = get_all_sequencing_ids(get_sequencing_ids())
    get_all_analysis_ids_task = get_all_analysis_ids(get_sequencing_ids())
    get_job_hash_task = get_job_hash(get_all_analysis_ids_task)

    prepare_nextflow_exomiser_task = exomiser.prepare(sequencing_ids=get_all_sequencing_ids_task)
    prepare_nextflow_post_processing_task = post_processing.prepare(
        seq_id_pheno_file_mapping=prepare_nextflow_exomiser_task,
        job_hash=get_job_hash_task
    )
    nextflow_post_processing_task = post_processing.run(
        input=prepare_nextflow_post_processing_task,
        job_hash=get_job_hash_task
    )
    
    add_exomiser_references_task = PipelineOperator(
        task_id='add_exomiser_references_task',
        name='add_exomiser_references_task',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        max_active_tis_per_dag=10,
        arguments=[
            'bio.ferlab.clin.etl.AddNextflowDocuments',
            prepare_exomiser_references_analysis_ids(get_all_analysis_ids_task),
            f'{nextflow_bucket}/{nextflow_post_processing_exomiser_output_key}',
        ]
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    (
        start >> params_validate >>
        ingest_fhir_group >>
        get_all_sequencing_ids_task >> get_job_hash_task >>
        [prepare_nextflow_exomiser_task, prepare_nextflow_post_processing_task] >>
        nextflow_post_processing_task >> add_exomiser_references_task >>
        slack
    )
