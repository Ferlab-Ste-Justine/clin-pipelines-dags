from lib.config import indexer_context, es_url, env
from lib.operators.spark import SparkOperator


def gene_centric(release_id: str, color: str, spark_jar: str, skip: str = '', task_id='gene_centric',
                 **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name='etl-index-gene-centric',
        k8s_context=indexer_context,
        spark_class='bio.ferlab.clin.etl.es.Indexer',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_gene_centric',
            release_id,
            'gene_centric_template.json',
            'gene_centric',
            '1900-01-01 00:00:00',
            f'config/{env}.conf',
        ],
        **kwargs
    )


def gene_suggestions(release_id: str, color: str, spark_jar: str, skip: str = '', task_id='gene_suggestions',
                     **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name='etl-index-gene-suggestions',
        k8s_context=indexer_context,
        spark_class='bio.ferlab.clin.etl.es.Indexer',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_gene_suggestions',
            release_id,
            'gene_suggestions_template.json',
            'gene_suggestions',
            '1900-01-01 00:00:00',
            f'config/{env}.conf',
        ],
        **kwargs
    )


def variant_centric(release_id: str, color: str, spark_jar: str, skip: str = '', task_id='variant_centric',
                    **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name='etl-index-variant-centric',
        k8s_context=indexer_context,
        spark_class='bio.ferlab.clin.etl.es.Indexer',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_variant_centric',
            release_id,
            'variant_centric_template.json',
            'variant_centric',
            '1900-01-01 00:00:00',
            f'config/{env}.conf',
        ],
        **kwargs
    )


def variant_suggestions(release_id: str, color: str, spark_jar: str, skip: str = '', task_id='variant_suggestions',
                        **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name='etl-index-variant-suggestions',
        k8s_context=indexer_context,
        spark_class='bio.ferlab.clin.etl.es.Indexer',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_variant_suggestions',
            release_id,
            'variant_suggestions_template.json',
            'variant_suggestions',
            '1900-01-01 00:00:00',
            f'config/{env}.conf',
        ],
        **kwargs
    )


def cnv_centric(release_id: str, color: str, spark_jar: str, skip: str = '', task_id='cnv_centric',
                **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name='etl-index-cnv-centric',
        k8s_context=indexer_context,
        spark_class='bio.ferlab.clin.etl.es.Indexer',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_cnv_centric',
            release_id,
            'cnv_centric_template.json',
            'cnv_centric',
            '1900-01-01 00:00:00',
            f'config/{env}.conf',
        ],
        **kwargs
    )


def coverage_by_gene_centric(release_id: str, color: str, spark_jar: str, skip: str = '',
                             task_id='coverage_by_gene_centric', **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name='etl-index-coverage-by-gene',
        k8s_context=indexer_context,
        spark_class='bio.ferlab.clin.etl.es.Indexer',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_coverage_by_gene_centric',
            release_id,
            'coverage_by_gene_centric_template.json',
            'coverage_by_gene_centric',
            '1900-01-01 00:00:00',
            f'config/{env}.conf',
        ],
        **kwargs
    )
