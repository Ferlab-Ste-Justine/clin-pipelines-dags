from lib.operators.spark_etl import SparkETLOperator

PREPARE_INDEX_MAIN_CLASS = 'bio.ferlab.clin.etl.es.PrepareIndex'


def gene_centric(spark_jar: str, skip: str = '') -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='gene_centric',
        task_id='gene_centric',
        name='etl-prepare-gene-centric',
        steps='initial',
        app_name='etl_prepare_gene_centric',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip
    )


def gene_suggestions(spark_jar: str, skip: str = '') -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='gene_suggestions',
        task_id='gene_suggestions',
        name='etl-prepare-gene-suggestions',
        steps='initial',
        app_name='etl_prepare_gene_suggestions',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip
    )


def variant_centric(spark_jar: str, skip: str = '') -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='variant_centric',
        task_id='variant_centric',
        name='etl-prepare-variant-centric',
        steps='initial',
        app_name='etl_prepare_variant_centric',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip
    )


def variant_suggestions(spark_jar: str, skip: str = '') -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='variant_suggestions',
        task_id='variant_suggestions',
        name='etl-prepare-variant-suggestions',
        steps='initial',
        app_name='etl_prepare_variant_suggestions',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip
    )


def cnv_centric(spark_jar: str, skip: str = '') -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='cnv_centric',
        task_id='cnv_centric',
        name='etl-prepare-cnv-centric',
        steps='initial',
        app_name='etl_prepare_cnv_centric',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip
    )


def coverage_by_gene_centric(spark_jar: str, skip: str = '') -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='coverage_by_gene_centric',
        task_id='coverage_by_gene_centric',
        name='etl-prepare-coverage-by-gene-centric',
        steps='initial',
        app_name='etl_prepare_coverage_by_gene_centric',
        spark_class=PREPARE_INDEX_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip
    )
