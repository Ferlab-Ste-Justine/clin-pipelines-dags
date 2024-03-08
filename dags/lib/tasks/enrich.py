from typing import List

from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator


class EnrichedSparkOperator(SparkOperator):
    def __init__(self,
                 first_arg: str,
                 steps: str,
                 app_name: str,
                 spark_config: str,
                 batch_id: str = '',
                 spark_jar: str = '',
                 skip: str = '',
                 **kwargs
                 ) -> None:
        super().__init__(
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.enriched.RunEnriched',
            spark_config=spark_config,
            spark_jar=spark_jar,
            skip=skip,
            **kwargs)
        arguments = [
            first_arg,
            '--config', config_file,
            '--steps', steps,
            '--app-name', app_name,
        ]
        if batch_id:
            arguments.append('--batchId')
            arguments.append(batch_id)

        self.arguments = arguments


def snv(steps: str, spark_jar: str = '', task_id: str = 'snv', name: str = 'etl-enrich-snv',
        app_name: str = 'etl_enrich_snv', skip: str = '', **kwargs) -> SparkOperator:
    return EnrichedSparkOperator(
        first_arg='snv',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def snv_somatic_all(steps: str, spark_jar: str = '', task_id: str = 'snv_somatic_all',
                    name: str = 'etl-enrich-snv-somatic-all', app_name: str = 'etl_enrich_snv_somatic_all',
                    skip: str = '', **kwargs):
    return EnrichedSparkOperator(
        first_arg='snv_somatic',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def snv_somatic(batch_ids: List[str], steps: str, spark_jar: str = '', task_id: str = 'snv_somatic',
                name: str = 'etl-enrich-snv-somatic', app_name: str = 'etl_enrich_snv_somatic', skip: str = '',
                **kwargs):
    return EnrichedSparkOperator.partial(
        first_arg='snv_somatic',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_config='config-etl-medium',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    ).expand(batch_id=batch_ids)


def variants(steps: str, spark_jar: str = '', task_id: str = 'variants', name: str = 'etl-enrich-variants',
             app_name: str = 'etl_enrich_variants', skip: str = '', **kwargs) -> SparkOperator:
    return EnrichedSparkOperator(
        first_arg='variants',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def consequences(steps: str, spark_jar: str = '', task_id: str = 'consequences', name: str = 'etl-enrich-consequences',
                 app_name: str = 'etl_enrich_consequences', skip: str = '', **kwargs) -> SparkOperator:
    return EnrichedSparkOperator(
        first_arg='consequences',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def cnv(steps: str, spark_jar: str = '', task_id: str = 'cnv', name: str = 'etl-enrich-cnv',
        app_name: str = 'etl_enrich_cnv', skip: str = '', **kwargs) -> SparkOperator:
    return EnrichedSparkOperator(
        first_arg='cnv',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )


def coverage_by_gene(steps: str, spark_jar: str = '', task_id: str = 'coverage_by_gene',
                     name: str = 'etl-enrich-coverage-by-gene',
                     app_name: str = 'etl_enrich_coverage_by_gene', skip: str = '', **kwargs) -> SparkOperator:
    return EnrichedSparkOperator(
        first_arg='coverage_by_gene',
        task_id=task_id,
        name=name,
        steps=steps,
        app_name=app_name,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        **kwargs
    )
