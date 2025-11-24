from typing import Set
import io

from airflow.decorators import task
from lib.config_nextflow import (
    nextflow_svclustering_parental_origin_revision,
    nextflow_svclustering_parental_origin_pipeline,
    nextflow_bucket,
    nextflow_svclustering_parental_origin_del_output_key,
    nextflow_svclustering_parental_origin_dup_output_key,
    nextflow_svclustering_parental_origin_info_output_key,
    nextflow_svclustering_parental_origin_input_key,
    NEXTFLOW_MAIN_CLASS
)
from lib.config_operators import nextflow_svclustering_base_config
from lib.datasets import enriched_clinical
from lib.operators.nextflow import NextflowOperator
from lib.operators.spark_etl import SparkETLOperator
from lib.utils import SKIP_EXIT_CODE


@task.virtualenv(skip_on_exit_code=SKIP_EXIT_CODE, task_id='prepare_svclustering_parental_origin', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
def prepare(analysis_ids: Set[str], job_hash: str, skip: str = ''):
    import sys
    import logging
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from pandas import DataFrame

    from lib.config import s3_conn_id
    from lib.utils_etl_tables import to_pandas

    if skip:
        sys.exit(SKIP_EXIT_CODE)

    s3 = S3Hook(s3_conn_id)

    # extract enrich clinical data
    df: DataFrame = to_pandas(enriched_clinical.uri)
    df = df[df['analysis_id'].isin(analysis_ids) & (df['bioinfo_analysis_code'] == 'GEBA')]
    df = df[['analysis_id', 'aliquot_id', 'father_aliquot_id', 'mother_aliquot_id', 'cnv_vcf_germline_urls']]

    # transform to samplesheet
    analysesWithAtLeastOneParent = df[df['mother_aliquot_id'].notnull() | df['father_aliquot_id'].notnull()]['analysis_id'].unique()
    df = df[df['cnv_vcf_germline_urls'].notnull() & df['analysis_id'].isin(analysesWithAtLeastOneParent)]
    column_map = {
        'analysis_id': 'familyId',
        'aliquot_id': 'sample',
        'cnv_vcf_germline_urls': 'vcf'
    }
    samples = df.rename(columns=column_map)[[*column_map.values()]]
    samples['vcf'] = samples['vcf'].str[0].str.replace('s3a://', 's3://', 1)
    if samples.empty:
        logging.info(f"No applicable samples found for analyses {analysis_ids}, skipping svclustering_parental_origin preparation.")
        return ""

    # Upload samplesheet CSV file to S3
    s3_key = nextflow_svclustering_parental_origin_input_key(job_hash)
    file_path = f"s3://{nextflow_bucket}/{s3_key}"
    with io.StringIO() as sio:
        samples.to_csv(sio, index=False)
        s3.load_string(
            string_data=sio.getvalue(),
            key=s3_key,
            bucket_name=nextflow_bucket,
            replace=True
        )
    logging.info(f"Samplesheet file for analyses {analysis_ids} uploaded to S3 path: {file_path}")
    return file_path


def run(job_hash: str, skip: str = '', **kwargs):
    return nextflow_svclustering_base_config \
        .with_pipeline(nextflow_svclustering_parental_origin_pipeline) \
        .with_revision(nextflow_svclustering_parental_origin_revision) \
        .append_args(
            '--input', f's3://{nextflow_bucket}/{nextflow_svclustering_parental_origin_input_key(job_hash)}',
            '--outdir', f's3://{nextflow_bucket}/{nextflow_svclustering_parental_origin_info_output_key(job_hash)}',
            '--dup_outdir', f's3://{nextflow_bucket}/{nextflow_svclustering_parental_origin_dup_output_key}',
            '--del_outdir', f's3://{nextflow_bucket}/{nextflow_svclustering_parental_origin_del_output_key}'
        ) \
        .operator(
            NextflowOperator,
            task_id='svclustering_parental_origin',
            name='svclustering_parental_origin',
            skip=skip,
            **kwargs
        )


def normalize(batch_id: str, analysis_ids: list, spark_jar: str, skip: str = '', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='normalize_svclustering_parental_origin',
        task_id='normalize_svclustering_parental_origin',
        name='normalize-svclustering-parental-origin',
        steps='default',
        app_name='normalize_svclustering_parental_origin',
        spark_class=NEXTFLOW_MAIN_CLASS,
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        batch_id_deprecated=True,
        **kwargs
    )
