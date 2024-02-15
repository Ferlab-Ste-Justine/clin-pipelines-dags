from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator


def snv(batch_id: str, spark_jar: str, skip: bool) -> SparkOperator:
    return SparkOperator(
        task_id='snv',
        name='etl-normalize-snv',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
        spark_config='config-etl-large',
        skip=skip,
        spark_jar=spark_jar,
        arguments=[
            'snv',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_normalize_snv',
            '--batchId', batch_id
        ],
    )


def snv_somatic(batch_id: str, spark_jar: str, skip: bool) -> SparkOperator:
    return SparkOperator(
        task_id='snv_somatic',
        name='etl-normalize-snv-somatic',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
        spark_config='config-etl-large',
        skip=skip,
        spark_jar=spark_jar,
        arguments=[
            'snv',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_normalize_snv_somatic',
            '--batchId', batch_id
        ],
    )


def cnv(batch_id: str, spark_jar: str, skip: bool) -> SparkOperator:
    return SparkOperator(
        task_id='cnv',
        name='etl-normalize-cnv',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
        spark_config='config-etl-large',
        skip=skip,
        spark_jar=spark_jar,
        arguments=[
            'cnv',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_normalize_cnv',
            '--batchId', batch_id
        ],
    )


def cnv_somatic_tumor_only(batch_id: str, spark_jar: str, skip: bool) -> SparkOperator:
    return SparkOperator(
        task_id='cnv_somatic_tumor_only',
        name='etl-normalize-cnv_somatic-tumor-only',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
        spark_config='config-etl-large',
        skip=skip,
        spark_jar=spark_jar,
        arguments=[
            'cnv',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_normalize_cnv_somatic',
            '--batchId', batch_id
        ],
    )


def variants(batch_id: str, spark_jar: str, skip: bool) -> SparkOperator:
    return SparkOperator(
        task_id='variants',
        name='etl-normalize-variants',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
        spark_config='config-etl-large',
        skip=skip,
        spark_jar=spark_jar,
        arguments=[
            'variants',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_normalize_variants',
            '--batchId', batch_id
        ],
    )


def consequences(batch_id: str, spark_jar: str, skip: bool) -> SparkOperator:
    return SparkOperator(
        task_id='consequences',
        name='etl-normalize-consequences',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
        spark_config='config-etl-large',
        skip=skip,
        spark_jar=spark_jar,
        arguments=[
            'consequences',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_normalize_consequences',
            '--batchId', batch_id
        ],
    )


def coverage_by_gene(batch_id: str, spark_jar: str, skip: bool) -> SparkOperator:
    return SparkOperator(
        task_id='coverage_by_gene',
        name='etl-normalize-coverage-by-gene',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
        spark_config='config-etl-large',
        skip=skip,
        spark_jar=spark_jar,
        arguments=[
            'coverage_by_gene',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_ingest_normalize_by_gene',
            '--batchId', batch_id
        ],
    )


def exomiser(batch_id: str, spark_jar: str, skip: bool) -> SparkOperator:
    return SparkOperator(
        task_id='exomiser',
        name='etl-normalize-exomiser',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
        spark_config='config-etl-large',
        skip=skip,
        spark_jar=spark_jar,
        arguments=[
            'exomiser',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_normalize_exomiser',
            '--batchId', batch_id
        ],
    )


def franklin(batch_id: str, spark_jar: str, skip: bool) -> SparkOperator:
    return SparkOperator(
        task_id='franklin',
        name='etl-normalize-franklin',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
        spark_config='config-etl-large',
        skip=skip,
        spark_jar=spark_jar,
        arguments=[
            'franklin',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl-normalize-franklin',
            '--batchId', batch_id
        ],
    )

