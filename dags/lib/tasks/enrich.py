from typing import List

from lib.config import chromosomes
from lib.operators.spark_etl import SparkETLOperator
from lib.utils_etl import ClinAnalysis

ENRICHED_MAIN_CLASS = 'bio.ferlab.clin.etl.enriched.RunEnriched'


def snv(steps: str, spark_jar: str = '', task_id: str = 'snv', name: str = 'etl-enrich-snv',
        app_name: str = 'etl_enrich_snv', skip: str = '', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='snv',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_class=ENRICHED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def snv_somatic_all(steps: str, spark_jar: str = '', task_id: str = 'snv_somatic_all',
                    name: str = 'etl-enrich-snv-somatic-all', app_name: str = 'etl_enrich_snv_somatic_all',
                    skip: str = '', **kwargs):
    return SparkETLOperator(
        entrypoint='snv_somatic',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_class=ENRICHED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def snv_somatic(batch_ids: List[str], steps: str, spark_jar: str = '', task_id: str = 'snv_somatic',
                name: str = 'etl-enrich-snv-somatic', app_name: str = 'etl_enrich_snv_somatic', skip: str = '',
                target_batch_types: List[ClinAnalysis] = None, **kwargs):
    return SparkETLOperator.partial(
        entrypoint='snv_somatic',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_class=ENRICHED_MAIN_CLASS,
        spark_config='config-etl-medium',
        spark_jar=spark_jar,
        skip=skip,
        target_batch_types=target_batch_types,
        max_active_tis_per_dag=1,  # Prevent multiple executions at the same time
        **kwargs
    ).expand(batch_id=batch_ids)


def variants(spark_jar: str = '', task_id: str = 'variants', name: str = 'etl-enrich-variants',
             app_name: str = 'etl_enrich_variants', skip: str = '', **kwargs) -> SparkETLOperator:
    return SparkETLOperator.partial(
        entrypoint='variants',
        task_id=task_id,
        name=name,
        steps='default',
        app_name=app_name,
        spark_class=ENRICHED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        max_active_tis_per_dag=1,  # concurrent OverWritePartition doesnt work
        **kwargs
    ).expand(chromosome=chromosomes)


def consequences(steps: str, spark_jar: str = '', task_id: str = 'consequences',
                 name: str = 'etl-enrich-consequences',
                 app_name: str = 'etl_enrich_consequences', skip: str = '', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='consequences',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_class=ENRICHED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def cnv_all(steps: str, spark_jar: str = '', task_id: str = 'cnv_all', name: str = 'etl-enrich-cnv-all',
            app_name: str = 'etl_enrich_cnv_all', skip: str = '', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='cnv',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_class=ENRICHED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def cnv(batch_ids: List[str], steps: str, spark_jar: str = '', task_id: str = 'cnv', name: str = 'etl-enrich-cnv',
        app_name: str = 'etl_enrich_cnv', skip: str = '', target_batch_types: List[ClinAnalysis] = None, **kwargs):
    return SparkETLOperator.partial(
        entrypoint='cnv',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_class=ENRICHED_MAIN_CLASS,
        spark_config='config-etl-medium',
        spark_jar=spark_jar,
        skip=skip,
        target_batch_types=target_batch_types,
        max_active_tis_per_dag=1,  # Prevent multiple executions at the same time
        **kwargs
    ).expand(batch_id=batch_ids)


def coverage_by_gene(steps: str, spark_jar: str = '', task_id: str = 'coverage_by_gene',
                     name: str = 'etl-enrich-coverage-by-gene',
                     app_name: str = 'etl_enrich_coverage_by_gene', skip: str = '', **kwargs) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='coverage_by_gene',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_class=ENRICHED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )
