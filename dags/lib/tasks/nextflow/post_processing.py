from typing import Dict

from airflow.decorators import task

from lib.config_nextflow import (
    nextflow_bucket,
    nextflow_post_processing_pipeline, nextflow_post_processing_revision,
    nextflow_post_processing_config_file, nextflow_post_processing_config_map,
    nextflow_post_processing_params_file,
    nextflow_post_processing_info_output_key,
    nextflow_post_processing_vep_output_key,
    nextflow_post_processing_exomiser_output_key
)
from lib.config_operators import nextflow_base_config
from lib.datasets import enriched_clinical
from lib.operators.nextflow import NextflowOperator


@task.virtualenv(task_id='prepare_post_processing', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
def prepare(seq_id_pheno_file_mapping: Dict[str, str], job_hash: str) -> str:
    """
    Prepare a samplesheet file for nextflow post-processing pipeline.

    Construct a single samplesheet file for all sequencing IDs in the input mapping using `enriched_clinical` table
    and upload it to S3 as a CSV file. The file contains the following columns:
      - `familyId`: analysis ID
      - `sample`: aliquot ID
      - `sequencingType`: sequencing strategy (only WES is supported at the moment)
      - `gvcf`: S3 URL of the gvcf file
      - `familyPheno`: S3 URL of the phenopacket file

    :param seq_id_pheno_file_mapping: Mapping of sequencing IDs to the S3 path of the corresponding phenopacket file
    :param job_hash: Unique hash for the job
    :return: S3 path of the samplesheet file
    """
    import io
    import logging
    from pandas import DataFrame
    from airflow.exceptions import AirflowFailException
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    from lib.config import s3_conn_id
    from lib.config_nextflow import nextflow_bucket, nextflow_post_processing_input_key
    from lib.datasets import enriched_clinical
    from lib.utils_etl_tables import to_pandas

    s3 = S3Hook(s3_conn_id)
    df: DataFrame = to_pandas(enriched_clinical.uri)

    column_map = {
        'analysis_id': 'familyId',
        'sequencing_id': 'sequencingId',
        'aliquot_id': 'sample',
        'sequencing_strategy': 'sequencingType',
        'snv_vcf_urls': 'gvcf'
    }

    samples = df[df['sequencing_id'].isin(seq_id_pheno_file_mapping.keys())] \
        .rename(columns=column_map)[[*column_map.values()]]

    # Replace sequencing strategy with 'WES' because Nextflow expects 'WES' instead of 'WXS'
    def set_sequencing_type(x):
        if x == 'WXS':
            return 'WES'
        else:
            raise AirflowFailException(f"Unsupported sequencing strategy: {x}. Only WES (WXS) is supported at the moment.")

    samples['sequencingType'] = samples['sequencingType'].apply(set_sequencing_type)

    # snv_vcf_urls (gvcf) is a list of URLs, we only need the first one
    # Nextflow only supports s3:// URLs
    samples['gvcf'] = samples['gvcf'].str[0].str.replace('s3a://', 's3://', 1)
    samples['familyPheno'] = samples['sequencingId'].map(seq_id_pheno_file_mapping)
    samples.drop(columns=['sequencingId'], inplace=True)

    s3_key = nextflow_post_processing_input_key(job_hash)
    file_path = f"s3://{nextflow_bucket}/{s3_key}"

    # Upload samplesheet CSV file to S3
    with io.StringIO() as sio:
        samples.to_csv(sio, index=False)
        s3.load_string(
            string_data=sio.getvalue(),
            key=s3_key,
            bucket_name=nextflow_bucket,
            replace=True
        )

    all_analysis_ids = samples['familyId'].unique().sort()
    logging.info(f"Samplesheet file for analyses {all_analysis_ids} uploaded to S3 path: {file_path}")
    return file_path


def run(input: str, job_hash: str, skip: str = '', **kwargs):
    """
    Executes the Nextflow post-processing pipeline.

    :param input: S3 path of the samplesheet file
    :param job_hash: Unique hash for the job
    :param skip: Skip the task
    """
    info_output_s3_key = nextflow_post_processing_info_output_key(job_hash)
    info_output_dir = f's3://{nextflow_bucket}/{info_output_s3_key}'
    vep_output_dir = f's3://{nextflow_bucket}/{nextflow_post_processing_vep_output_key}'
    exomiser_output_dir = f's3://{nextflow_bucket}/{nextflow_post_processing_exomiser_output_key}'

    return nextflow_base_config \
        .with_pipeline(nextflow_post_processing_pipeline) \
        .with_revision(nextflow_post_processing_revision) \
        .append_config_maps(nextflow_post_processing_config_map) \
        .append_config_files(nextflow_post_processing_config_file) \
        .with_params_file(nextflow_post_processing_params_file) \
        .append_args(
            '--input', input,
            '--outdir', info_output_dir,
            '--vep_outdir', vep_output_dir,
            '--exomiser_outdir', exomiser_output_dir,
        ) \
        .operator(
            NextflowOperator,
            task_id='post_processing',
            name='post_processing',
            skip=skip,
            **kwargs
        )
