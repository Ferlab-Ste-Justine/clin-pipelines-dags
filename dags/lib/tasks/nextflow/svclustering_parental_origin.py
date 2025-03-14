from lib.config_nextflow import nextflow_svclustering_parental_origin_revision, \
    nextflow_svclustering_parental_origin_pipeline, nextflow_bucket, nextflow_svclustering_parental_origin_input_key, \
    NEXTFLOW_MAIN_CLASS
from lib.config_operators import nextflow_svclustering_base_config
from lib.operators.nextflow import NextflowOperator
from lib.operators.spark_etl import SparkETLOperator


def prepare(batch_id: str, spark_jar: str, skip: str = '', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='prepare_svclustering_parental_origin',
        task_id='prepare_svclustering_parental_origin',
        name='prepare-svclustering-parental-origin',
        steps='default',
        app_name='prepare_svclustering_parental_origin',
        spark_class=NEXTFLOW_MAIN_CLASS,
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        **kwargs
    )


def run(batch_id: str, skip: str = '', **kwargs):
    return nextflow_svclustering_base_config \
        .with_pipeline(nextflow_svclustering_parental_origin_pipeline) \
        .with_revision(nextflow_svclustering_parental_origin_revision) \
        .append_args(
            '--input', f's3://{nextflow_bucket}/{nextflow_svclustering_parental_origin_input_key(batch_id)}',
            '--outdir', f's3://{nextflow_bucket}/nextflow/svclustering_parental_origin_output/{batch_id}') \
        .operator(
            NextflowOperator,
            task_id='svclustering_parental_origin',
            name='svclustering_parental_origin',
            skip=skip,
            **kwargs
        )


def normalize(batch_id: str, spark_jar: str, skip: str = '', **kwargs) -> SparkETLOperator:
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
        **kwargs
    )