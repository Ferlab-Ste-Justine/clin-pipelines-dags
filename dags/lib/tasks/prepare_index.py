from lib.operators.spark_etl import SparkETLOperator
from lib.config import Env, chromosomes, env

PREPARE_INDEX_MAIN_CLASS = 'bio.ferlab.clin.etl.es.PrepareIndex'


def gene_centric(spark_jar: str, skip: str = '', task_id: str = 'gene_centric', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='gene_centric',
        task_id=task_id,
        name='etl-prepare-gene-centric',
        steps='initial',
        app_name='etl_prepare_gene_centric',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def gene_suggestions(spark_jar: str, skip: str = '', task_id: str = 'gene_suggestions', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='gene_suggestions',
        task_id=task_id,
        name='etl-prepare-gene-suggestions',
        steps='initial',
        app_name='etl_prepare_gene_suggestions',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip
    )


def variant_centric(spark_jar: str, skip: str = '', task_id: str = 'variant_centric', **kwargs) -> SparkETLOperator:
    # we dont have the resources in PROD to prepare all chromosomes at once, we have to split
    if env == Env.PROD:
        return SparkETLOperator.partial(
            entrypoint='variant_centric',
            task_id=task_id,
            name='etl-prepare-variant-centric',
            steps='default',
            app_name='etl_prepare_variant_centric',
            spark_class=PREPARE_INDEX_MAIN_CLASS,
            spark_config='config-etl-large',
            spark_jar=spark_jar,
             max_active_tis_per_dag=1,  # concurrent OverWritePartition doesnt work
            skip=skip,
            **kwargs
        ).expand(chromosome=chromosomes)
    else:
        return SparkETLOperator(
            entrypoint='variant_centric',
            task_id=task_id,
            name='etl-prepare-variant-centric',
            steps='initial',
            app_name='etl_prepare_variant_centric',
            spark_class=PREPARE_INDEX_MAIN_CLASS,
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            skip=skip,
            **kwargs
        )


def variant_suggestions(spark_jar: str, skip: str = '', task_id: str = 'variant_suggestions', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='variant_suggestions',
        task_id=task_id,
        name='etl-prepare-variant-suggestions',
        steps='initial',
        app_name='etl_prepare_variant_suggestions',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def cnv_centric(spark_jar: str, skip: str = '', task_id: str = 'cnv_centric', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='cnv_centric',
        task_id=task_id,
        name='etl-prepare-cnv-centric',
        steps='initial',
        app_name='etl_prepare_cnv_centric',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def coverage_by_gene_centric(spark_jar: str, skip: str = '', task_id: str = 'coverage_by_gene_centric', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='coverage_by_gene_centric',
        task_id=task_id,
        name='etl-prepare-coverage-by-gene-centric',
        steps='initial',
        app_name='etl_prepare_coverage_by_gene_centric',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )
