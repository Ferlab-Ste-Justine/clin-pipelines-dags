from lib.config import K8sContext, Env, es_url, env
from lib.operators.spark import SparkOperator


def gene_centric(release_id: str, color: str, spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='gene_centric',
        name='etl-publish-gene-centric',
        k8s_context=K8sContext.DEFAULT,
        spark_class='bio.ferlab.clin.etl.es.Publish',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_gene_centric',
            release_id,
        ],
    )


def gene_suggestions(release_id: str, color: str, spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='gene_suggestions',
        name='etl-publish-gene-suggestions',
        k8s_context=K8sContext.DEFAULT,
        spark_class='bio.ferlab.clin.etl.es.Publish',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_gene_suggestions',
            release_id,
        ],
    )


def variant_centric(release_id: str, color: str, spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='variant_centric',
        name='etl-publish-variant-centric',
        k8s_context=K8sContext.DEFAULT,
        spark_class='bio.ferlab.clin.etl.es.Publish',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_variant_centric',
            release_id,
        ],
    )


def variant_suggestions(release_id: str, color: str, spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='variant_suggestions',
        name='etl-publish-variant-suggestions',
        k8s_context=K8sContext.DEFAULT,
        spark_class='bio.ferlab.clin.etl.es.Publish',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_variant_suggestions',
            release_id,
        ],
    )


def cnv_centric(release_id: str, color: str, spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='cnv_centric',
        name='etl-publish-cnv-centric',
        k8s_context=K8sContext.DEFAULT,
        spark_class='bio.ferlab.clin.etl.es.Publish',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_cnv_centric',
            release_id,
        ],
    )


def coverage_by_gene_centric(release_id: str, color: str, spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='coverage_by_gene_centric',
        name='etl-publish-cnv-centric',
        k8s_context=K8sContext.DEFAULT,
        spark_class='bio.ferlab.clin.etl.es.Publish',
        spark_config='config-etl-singleton',
        spark_jar=spark_jar,
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        arguments=[
            es_url, '', '',
            f'clin_{env}' + color + '_coverage_by_gene_centric',
            release_id,
        ],
    )
