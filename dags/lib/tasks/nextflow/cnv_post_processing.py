from lib.config_nextflow import (
    nextflow_bucket,
    nextflow_cnv_post_processing_pipeline,
    nextflow_cnv_post_processing_revision,
    nextflow_cnv_post_processing_config_file,
    nextflow_cnv_post_processing_config_map,
    nextflow_cnv_post_processing_params_file,
    nextflow_cnv_post_processing_info_output_key,
    nextflow_cnv_post_processing_exomiser_output_key
)
from lib.config_operators import nextflow_base_config
from lib.operators.nextflow import NextflowOperator


def run(input: str, job_hash: str, skip: str = '', **kwargs):
    """
    Executes the Nextflow post-processing pipeline.

    :param input: S3 path of the samplesheet file
    :param job_hash: Unique hash for the job
    :param skip: Skip the task
    """
    info_output_s3_key = nextflow_cnv_post_processing_info_output_key(job_hash)
    info_output_dir = f's3://{nextflow_bucket}/{info_output_s3_key}'
    exomiser_output_dir = f's3://{nextflow_bucket}/{nextflow_cnv_post_processing_exomiser_output_key}'

    return nextflow_base_config \
        .with_pipeline(nextflow_cnv_post_processing_pipeline) \
        .with_revision(nextflow_cnv_post_processing_revision) \
        .append_config_maps(nextflow_cnv_post_processing_config_map) \
        .append_config_files(nextflow_cnv_post_processing_config_file) \
        .with_params_file(nextflow_cnv_post_processing_params_file) \
        .append_args(
            '--input', input,
            '--outdir', info_output_dir,
            '--exomiser_outdir', exomiser_output_dir,
        ) \
        .operator(
            NextflowOperator,
            task_id='cnv_post_processing',
            name='cnv_post_processing',
            skip=skip,
            **kwargs
        )
