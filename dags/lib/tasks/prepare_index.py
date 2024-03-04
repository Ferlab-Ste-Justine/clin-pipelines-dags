from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator


def gene_centric(spark_jar: str, skip: str = '') -> SparkOperator:
    return SparkOperator(
        task_id='gene_centric',
        name='etl-prepare-gene-centric',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'gene_centric',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_gene_centric',
        ],
    )


def gene_suggestions(spark_jar: str, skip: str = '') -> SparkOperator:
    return SparkOperator(
        task_id='gene_suggestions',
        name='etl-prepare-gene-suggestions',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'gene_suggestions',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_gene_suggestions',
        ],
    )


def variant_centric(spark_jar: str, skip: str = '') -> SparkOperator:
    return SparkOperator(
        task_id='variant_centric',
        name='etl-prepare-variant-centric',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'variant_centric',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_variant_centric',
        ],
    )


def variant_suggestions(spark_jar: str, skip: str = '') -> SparkOperator:
    return SparkOperator(
        task_id='variant_suggestions',
        name='etl-prepare-variant-suggestions',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'variant_suggestions',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_variant_suggestions',
        ],
    )


def cnv_centric(spark_jar: str, skip: str = '') -> SparkOperator:
    return SparkOperator(
        task_id='cnv_centric',
        name='etl-prepare-cnv-centric',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'cnv_centric',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_cnv_centric',
        ],
    )


def coverage_by_gene_centric(spark_jar: str, skip: str = '') -> SparkOperator:
    return SparkOperator(
        task_id='coverage_by_gene',
        name='etl-prepare-coverage-by-gene',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'coverage_by_gene_centric',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_coverage_by_gene',
        ],
    )
