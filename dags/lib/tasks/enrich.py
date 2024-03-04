from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator


def snv(steps: str, spark_jar: str = '', task_id: str = 'snv', name: str = 'etl-enrich-snv',
        app_name: str = 'etl_enrich_snv', skip: str = '', **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name=name,
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'snv',
            '--config', config_file,
            '--steps', steps,
            '--app-name', app_name,
        ],
        **kwargs
    )


def snv_somatic(steps: str, spark_jar: str = '', task_id: str = 'snv_somatic', name: str = 'etl-enrich-snv-somatic',
                app_name: str = 'etl_enrich_snv_somatic', skip: str = '', **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name=name,
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
        spark_config='config-etl-medium',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'snv_somatic',
            '--config', config_file,
            '--steps', steps,
            '--app-name', app_name,
        ],
        **kwargs
    )


def variants(steps: str, spark_jar: str = '', task_id: str = 'variants', name: str = 'etl-enrich-variants',
             app_name: str = 'etl_enrich_variants', skip: str = '', **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name=name,
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'variants',
            '--config', config_file,
            '--steps', steps,
            '--app-name', app_name,
        ],
        **kwargs
    )


def consequences(steps: str, spark_jar: str = '', task_id: str = 'consequences', name: str = 'etl-enrich-consequences',
                 app_name: str = 'etl_enrich_consequences', skip: str = '', **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name=name,
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'consequences',
            '--config', config_file,
            '--steps', steps,
            '--app-name', app_name,
        ],
        **kwargs
    )


def cnv(steps: str, spark_jar: str = '', task_id: str = 'cnv', name: str = 'etl-enrich-cnv',
        app_name: str = 'etl_enrich_cnv', skip: str = '', **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name=name,
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'cnv',
            '--config', config_file,
            '--steps', steps,
            '--app-name', app_name,
        ],
        **kwargs
    )


def coverage_by_gene(steps: str, spark_jar: str = '', task_id: str = 'coverage_by_gene',
                     name: str = 'etl-enrich-coverage-by-gene',
                     app_name: str = 'etl_enrich_coverage_by_gene', skip: str = '', **kwargs) -> SparkOperator:
    return SparkOperator(
        task_id=task_id,
        name=name,
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        arguments=[
            'coverage_by_gene',
            '--config', config_file,
            '--steps', steps,
            '--app-name', app_name,
        ],
        **kwargs
    )
