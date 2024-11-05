from lib.config import clin_datalake_bucket
from lib.config_nextflow import nextflow_svclustering_revision, nextflow_svclustering_parental_origin_pipeline, \
    nextflow_svclustering_parental_origin_revision, nextflow_svclustering_pipeline
from lib.config_operators import nextflow_svclustering_base_config
from lib.operators.nextflow import NextflowOperator
from lib.operators.spark_etl import SparkETLOperator

NEXTFLOW_MAIN_CLASS = 'bio.ferlab.clin.etl.nextflow.RunNextflow'


################
# SVClustering #
################
def prepare_svclustering(spark_jar: str, skip: str = '', **kwargs) -> SparkETLOperator:
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


def svclustering(skip: str = '', **kwargs):
    return nextflow_svclustering_base_config \
        .with_pipeline(nextflow_svclustering_pipeline) \
        .with_revision(nextflow_svclustering_revision) \
        .append_args(
            '--input', f's3://{clin_datalake_bucket}/nextflow/svclustering_input/svclustering_input.csv',
            '--outdir', f's3://{clin_datalake_bucket}/nextflow/svclustering_output') \
        .operator(
            NextflowOperator,
            task_id='svclustering',
            name='svclustering',
            skip=skip,
            **kwargs
        )


def normalize_svclustering(spark_jar: str, skip: str = '', **kwargs) -> SparkETLOperator:
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


################################
# SVClustering Parental Origin #
################################
def prepare_svclustering_parental_origin(batch_id: str, spark_jar: str, skip: str = '', **kwargs) -> SparkETLOperator:
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


def svclustering_parental_origin(batch_id: str, skip: str = '', **kwargs):
    return nextflow_svclustering_base_config \
        .with_pipeline(nextflow_svclustering_parental_origin_pipeline) \
        .with_revision(nextflow_svclustering_parental_origin_revision) \
        .append_args(
            '--input', f's3://{clin_datalake_bucket}/nextflow/svclustering_parental_origin_input/{batch_id}/{batch_id}.csv',
            '--outdir', f's3://{clin_datalake_bucket}/nextflow/svclustering_parental_origin_output/{batch_id}') \
        .operator(
            NextflowOperator,
            task_id='svclustering_parental_origin',
            name='svclustering_parental_origin',
            skip=skip,
            **kwargs
        )


def normalize_svclustering_parental_origin(batch_id: str, spark_jar: str, skip: str = '', **kwargs) -> SparkETLOperator:
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
