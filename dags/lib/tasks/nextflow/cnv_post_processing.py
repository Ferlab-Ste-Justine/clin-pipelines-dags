from typing import Dict

from airflow.decorators import task

from lib.config_nextflow import (
    nextflow_bucket, nextflow_cnv_post_processing_config_file,
    nextflow_cnv_post_processing_config_map,
    nextflow_cnv_post_processing_exomiser_output_key,
    nextflow_cnv_post_processing_info_output_key,
    nextflow_cnv_post_processing_params_file,
    nextflow_cnv_post_processing_pipeline,
    nextflow_cnv_post_processing_revision)
from lib.config_operators import nextflow_base_config
from lib.datasets import enriched_clinical
from lib.operators.nextflow import NextflowOperator
from lib.utils import SKIP_EXIT_CODE


@task.virtualenv(skip_on_exit_code=SKIP_EXIT_CODE, task_id='cnv_prepare_post_processing', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
def prepare(analysis_id_pheno_file_mapping: Dict[str, str], job_hash: str, skip: str) -> str:
    """
    Prepare a samplesheet file for nextflow post-processing pipeline.

    Construct a single samplesheet file for all analysis IDs in the input mapping using `enriched_clinical` table
    and upload it to S3 as a CSV file. The file contains the following columns:
      - `sample`: aliquot ID
      - `sequencingType`: sequencing strategy (only WES is supported at the moment)
      - `vcf`: S3 URL of the gvcf file
      - `pheno`: S3 URL of the phenopacket file

    :param analysis_id_pheno_file_mapping: Mapping of analysis IDs to the S3 path of the corresponding phenopacket file
    :param job_hash: Unique hash for the job
    :return: S3 path of the samplesheet file
    """
    import io
    import logging
    import sys

    from airflow.exceptions import AirflowFailException
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from lib.config import s3_conn_id
    from lib.config_nextflow import (nextflow_bucket,
                                     nextflow_cnv_post_processing_input_key)
    from lib.datasets import enriched_clinical
    from lib.utils import SKIP_EXIT_CODE
    from lib.utils_etl_tables import to_pandas
    from pandas import DataFrame

    if skip:
        sys.exit(SKIP_EXIT_CODE)

    s3 = S3Hook(s3_conn_id)
    df: DataFrame = to_pandas(enriched_clinical.uri)

    column_map = {
        'analysis_id': 'sample',
        'sequencing_strategy': 'sequencingType',
        'cnv_vcf_germline_urls': 'vcf'
    }

    samples = df[df['analysis_id'].isin(analysis_id_pheno_file_mapping.keys()) & df["is_proband"]] \
        .rename(columns=column_map)[[*column_map.values()]]

    # Replace sequencing strategy with 'WES' because Nextflow expects 'WES' instead of 'WXS'
    def set_sequencing_type(x):
        if x == 'WXS':
            return 'WES'
        else:
            raise AirflowFailException(f"Unsupported sequencing strategy: {x}. Only WES (WXS) is supported at the moment.")

    samples['sequencingType'] = samples['sequencingType'].apply(set_sequencing_type)

    # cnv_vcf_germline_urls (vcf) is a list of URLs, we only need the first one
    # Nextflow only supports s3:// URLs
    samples['vcf'] = samples['vcf'].str[0].str.replace('s3a://', 's3://', 1)
    samples['pheno'] = samples['sample'].map(analysis_id_pheno_file_mapping)

    s3_key = nextflow_cnv_post_processing_input_key(job_hash)
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

    logging.info(f"Samplesheet file for analyses {analysis_id_pheno_file_mapping.keys()} uploaded to S3 path: {file_path}")
    return file_path


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
