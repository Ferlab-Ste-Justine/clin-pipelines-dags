from lib.config_nextflow import (
    nextflow_svclustering_input_key,
    nextflow_svclustering_output_key,
    nextflow_svclustering_revision,
    nextflow_svclustering_pipeline,
    nextflow_bucket,
    NEXTFLOW_MAIN_CLASS
)
from lib.config_operators import nextflow_svclustering_base_config
from lib.operators.nextflow import NextflowOperator
from lib.operators.spark_etl import SparkETLOperator


def prepare(spark_jar: str, skip: str = '', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='prepare_svclustering',
        task_id='prepare_svclustering',
        name='prepare-svclustering',
        steps='default',
        app_name='prepare_svclustering',
        spark_class=NEXTFLOW_MAIN_CLASS,
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def run(skip: str = '', **kwargs):
    return nextflow_svclustering_base_config \
        .with_pipeline(nextflow_svclustering_pipeline) \
        .with_revision(nextflow_svclustering_revision) \
        .append_args(
            '--input', f's3://{nextflow_bucket}/{nextflow_svclustering_input_key}',
            '--outdir', f's3://{nextflow_bucket}/{nextflow_svclustering_output_key}') \
        .operator(
            NextflowOperator,
            task_id='svclustering',
            name='svclustering',
            skip=skip,
            **kwargs
        )


def normalize(spark_jar: str, skip: str = '', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='normalize_svclustering',
        task_id='normalize_svclustering',
        name='normalize-svclustering',
        steps='default',
        app_name='normalize_svclusteringn',
        spark_class=NEXTFLOW_MAIN_CLASS,
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )
