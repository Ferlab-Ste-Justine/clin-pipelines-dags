from typing import List, Set

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowSkipException

from lib.config import K8sContext
from lib.config_nextflow import (
    nextflow_bucket, nextflow_cnv_post_processing_exomiser_output_key,
    nextflow_post_processing_exomiser_output_key)
from lib.datasets import enriched_clinical
from lib.operators.pipeline import PipelineOperator
from lib.tasks.nextflow import cnv_post_processing, exomiser, post_processing
from lib.utils import SKIP_EXIT_CODE
from lib.utils_etl import color, get_job_hash


@task_group(group_id='nextflow_germline')
def nextflow_germline(analysis_ids: List[str]):

    @task(task_id='check_should_skip')
    def check_should_skip(analysis_ids: List[str]) -> bool:
        """
        Determine if the nextflow germline tasks should be skipped based on the input analysis IDs.
        """
        return len(analysis_ids) == 0

    skip = check_should_skip(analysis_ids)

    @task.virtualenv(skip_on_exit_code=SKIP_EXIT_CODE, task_id='check_nextflow_germline_prerequisites', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical], max_active_tis_per_dag=1)
    def check_nextflow_germline_prerequisites(analysis_ids: List[str], skip: str) -> Set[str]:
        """
        Validates that:
        - all corresponding sequencing ids have an associated SNV VCF file
        - all proband corresponding sequencing ids have an associated CNV VCF file
        - all proband sequencing IDs have at least one associated clinical sign.
        """
        import sys
        from airflow.exceptions import AirflowFailException
        from lib.datasets import enriched_clinical
        from lib.utils import SKIP_EXIT_CODE
        from lib.utils_etl_tables import to_pandas

        from pandas import DataFrame

        if skip:
            sys.exit(SKIP_EXIT_CODE)  # Skip the task if skip is set

        df: DataFrame = to_pandas(enriched_clinical.uri)
        clinical_df = df[["analysis_id", "is_proband", "clinical_signs", "snv_vcf_germline_urls", "cnv_vcf_germline_urls", "sequencing_id"]]

        # Filter clinical_df for analysis IDs
        filtered_df = clinical_df[clinical_df["analysis_id"].isin(analysis_ids)]

        # Ensure we already have an associated snv vcf url
        missing_snv_seq_ids = set(filtered_df.loc[filtered_df["snv_vcf_germline_urls"].isna() |
                                                  (filtered_df["snv_vcf_germline_urls"].str.len() == 0), "sequencing_id"])
        if missing_snv_seq_ids:
            raise AirflowFailException(f"Some sequencing IDs don't have associated SNV VCF files: {missing_snv_seq_ids}")

        # Ensure we already have an associated cnv vcf url for probands
        missing_cnv_seq_ids = set(filtered_df.loc[(filtered_df["is_proband"]) &
                                                  (filtered_df["cnv_vcf_germline_urls"].isna() |
                                                   (filtered_df["cnv_vcf_germline_urls"].str.len() == 0)), "sequencing_id"])
        if missing_cnv_seq_ids:
            raise AirflowFailException(f"Some proband sequencing IDs don't have associated CNV VCF files: {missing_cnv_seq_ids}")

        # Ensure all proband sequencing IDs have at least one associated clinical sign
        missing_clinical_signs_seq_ids = set(filtered_df.loc[(filtered_df["is_proband"]) &
                                                             ((filtered_df["clinical_signs"].isna()) |
                                                              (filtered_df["clinical_signs"].str.len() == 0)), "sequencing_id"])
        if missing_clinical_signs_seq_ids:
            raise AirflowFailException(f"Some proband sequencing IDs don't have at least one associated clinical sign: {missing_clinical_signs_seq_ids}")

    @task(task_id='prepare_exomiser_references_analysis_ids')
    def prepare_exomiser_references_analysis_ids(analysis_ids: Set[str], skip: str) -> str:
        if skip:
            raise AirflowSkipException("Skipping exomiser references preparation task.")
        return '--analysis-ids=' + ','.join(analysis_ids)

    check_prerequisites_task = check_nextflow_germline_prerequisites(analysis_ids, skip=skip)
    get_job_hash_task = get_job_hash(analysis_ids=analysis_ids, skip=skip)
    prepare_exomiser_references_analysis_ids_task = prepare_exomiser_references_analysis_ids(analysis_ids, skip=skip)
    prepare_nextflow_exomiser_task = exomiser.prepare(analysis_ids=analysis_ids, skip=skip)

    @task_group(group_id='snv')
    def snv():
        prepare_nextflow_post_processing_task = post_processing.prepare(
            analysis_id_pheno_file_mapping=prepare_nextflow_exomiser_task,
            job_hash=get_job_hash_task,
            skip=skip
        )
        nextflow_post_processing_task = post_processing.run(
            input=prepare_nextflow_post_processing_task,
            job_hash=get_job_hash_task,
            skip=skip
        )
        add_exomiser_references_task = PipelineOperator(
            task_id='add_exomiser_references_task',
            name='add_exomiser_references_task',
            k8s_context=K8sContext.DEFAULT,
            color=color(),
            max_active_tis_per_dag=10,
            arguments=[
                'bio.ferlab.clin.etl.AddNextflowDocuments',
                prepare_exomiser_references_analysis_ids_task,
                '--nextflow-output-folder=' + f'{nextflow_bucket}/{nextflow_post_processing_exomiser_output_key}',
                '--exomiser-type=snv',
            ],
            skip=skip
        )
        prepare_nextflow_post_processing_task >> nextflow_post_processing_task >> add_exomiser_references_task

    @task_group(group_id='cnv')
    def cnv():
        prepare_nextflow_post_processing_task = cnv_post_processing.prepare(
            analysis_id_pheno_file_mapping=prepare_nextflow_exomiser_task,
            job_hash=get_job_hash_task,
            skip=skip
        )
        nextflow_post_processing_task = cnv_post_processing.run(
            input=prepare_nextflow_post_processing_task,
            job_hash=get_job_hash_task,
            skip=skip
        )
        add_exomiser_references_task = PipelineOperator(
            task_id='cnv_add_exomiser_references_task',
            name='cnv_add_exomiser_references_task',
            k8s_context=K8sContext.DEFAULT,
            color=color(),
            max_active_tis_per_dag=10,
            arguments=[
                'bio.ferlab.clin.etl.AddNextflowDocuments',
                prepare_exomiser_references_analysis_ids_task,
                '--nextflow-output-folder=' + f'{nextflow_bucket}/{nextflow_cnv_post_processing_exomiser_output_key}',
                '--exomiser-type=cnv',
            ],
            skip=skip
        )
        prepare_nextflow_post_processing_task >> nextflow_post_processing_task >> add_exomiser_references_task

    check_prerequisites_task >> get_job_hash_task >> prepare_exomiser_references_analysis_ids_task >> prepare_nextflow_exomiser_task >> snv() >> cnv()
