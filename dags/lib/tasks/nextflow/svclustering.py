from airflow.decorators import task
from lib.config_nextflow import (NEXTFLOW_MAIN_CLASS, nextflow_bucket,
                                 nextflow_svclustering_pipeline,
                                 nextflow_svclustering_revision)
from lib.config_operators import nextflow_svclustering_base_config
from lib.datasets import enriched_clinical
from lib.operators.nextflow import NextflowOperator
from lib.operators.spark_etl import SparkETLOperator


@task.virtualenv(task_id='prepare_svclustering', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
def prepare():
    import io
    import logging

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from lib.config import s3_conn_id
    from lib.config_nextflow import (nextflow_bucket,
                                     nextflow_svclustering_germline_input_key,
                                     nextflow_svclustering_somatic_input_key)
    from lib.datasets import enriched_clinical
    from lib.utils_etl_tables import to_pandas
    from pandas import DataFrame

    s3 = S3Hook(s3_conn_id)
    df: DataFrame = to_pandas(enriched_clinical.uri)
    filtered_df = df[df['cnv_vcf_germline_urls'].notnull() | df['cnv_vcf_somatic_urls'].notnull()]

    prepared_df = (
        filtered_df
        .assign(
            sample=lambda d: d['aliquot_id'],
            familyId=lambda d: d['analysis_id'],
            # Only a single URL is expected and Nextflow only supports s3:// URLs
            germline_vcf=lambda d: d['cnv_vcf_germline_urls'].str[0].str.replace('s3a://', 's3://', 1),
            somatic_vcf=lambda d: d['cnv_vcf_somatic_urls'].str[0].str.replace('s3a://', 's3://', 1),
        )[['sample', 'familyId', 'germline_vcf', 'somatic_vcf']]
        .drop_duplicates()
    )

    germline_df = (
        prepared_df
        .loc[prepared_df['germline_vcf'].notna(), ['sample', 'familyId', 'germline_vcf']]
        .rename(columns={'germline_vcf': 'vcf'})
    )
    somatic_df = (
        prepared_df
        .loc[prepared_df['somatic_vcf'].notna(), ['sample', 'familyId', 'somatic_vcf']]
        .rename(columns={'somatic_vcf': 'vcf'})
    )

    # Upload the germline samplesheet to S3
    with io.StringIO() as sio:
        germline_df.to_csv(sio, index=False)
        s3.load_string(
            string_data=sio.getvalue(),
            key=nextflow_svclustering_germline_input_key,
            bucket_name=nextflow_bucket,
            replace=True
        )
    logging.info(
        f"Samplesheet file for germline CNVs uploaded to S3 path: s3://{nextflow_bucket}/{nextflow_svclustering_germline_input_key}")

    # Upload the somatic samplesheet to S3
    with io.StringIO() as sio:
        somatic_df.to_csv(sio, index=False)
        s3.load_string(
            string_data=sio.getvalue(),
            key=nextflow_svclustering_somatic_input_key,
            bucket_name=nextflow_bucket,
            replace=True
        )
    logging.info(
        f"Samplesheet file for somatic CNVs uploaded to S3 path: s3://{nextflow_bucket}/{nextflow_svclustering_somatic_input_key}")


def run(input_key: str, output_key: str, task_id: str = 'svclustering', name: str = 'svclustering', skip: str = '',
        **kwargs):
    return (
        nextflow_svclustering_base_config
        .with_pipeline(nextflow_svclustering_pipeline)
        .with_revision(nextflow_svclustering_revision)
        .append_args(
            '--input', f's3://{nextflow_bucket}/{input_key}',
            '--outdir', f's3://{nextflow_bucket}/{output_key}'
        )
        .operator(
            NextflowOperator,
            task_id=task_id,
            name=name,
            skip=skip,
            **kwargs
        )
    )


def normalize(entrypoint: str, spark_jar: str, task_id: str, name: str, skip: str = '', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint=entrypoint,
        task_id=task_id,
        name=name,
        steps='default',
        app_name=entrypoint,  # Use entrypoint as app name since it follows our app name convention
        spark_class=NEXTFLOW_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )
