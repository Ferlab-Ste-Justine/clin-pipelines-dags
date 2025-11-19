from typing import List

from lib.operators.spark_etl import SparkETLOperator
from lib.utils_etl import ClinAnalysis

NORMALIZED_MAIN_CLASS = 'bio.ferlab.clin.etl.normalized.RunNormalized'


def snv(batch_id: str, analysis_ids: list, target_batch_types: List[ClinAnalysis], spark_jar: str, skip: str, detect_batch_type_task_id: str) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='snv',
        task_id='snv',
        name='etl-normalize-snv',
        steps='default',
        app_name='etl_normalize_snv',
        spark_class=NORMALIZED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        target_batch_types=target_batch_types,
        detect_batch_type_task_id=detect_batch_type_task_id,
        batch_id_deprecated=True
    )


def snv_somatic(batch_id: str, analysis_ids: list, bioinfo_analysis_code: str, target_batch_types: List[ClinAnalysis], spark_jar: str, skip: str, detect_batch_type_task_id: str) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='snv_somatic',
        task_id='snv_somatic',
        name='etl-normalize-snv-somatic',
        steps='default',
        app_name='etl_normalize_snv_somatic',
        spark_class=NORMALIZED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        bioinfo_analysis_code=bioinfo_analysis_code,
        target_batch_types=target_batch_types,
        detect_batch_type_task_id=detect_batch_type_task_id,
        batch_id_deprecated=True
    )


def cnv(batch_id: str, analysis_ids: list, target_batch_types: List[ClinAnalysis], spark_jar: str, skip: str, detect_batch_type_task_id: str) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='cnv',
        task_id='cnv',
        name='etl-normalize-cnv',
        steps='default',
        app_name='etl_normalize_cnv',
        spark_class=NORMALIZED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        target_batch_types=target_batch_types,
        detect_batch_type_task_id=detect_batch_type_task_id,
        batch_id_deprecated=True
    )


def cnv_somatic_tumor_only(batch_id: str, analysis_ids: list, target_batch_types: List[ClinAnalysis], spark_jar: str, skip: str, detect_batch_type_task_id: str) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='cnv_somatic_tumor_only',
        task_id='cnv_somatic_tumor_only',
        name='etl-normalize-cnv_somatic-tumor-only',
        steps='default',
        app_name='etl_normalize_cnv_somatic',
        spark_class=NORMALIZED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        target_batch_types=target_batch_types,
        detect_batch_type_task_id=detect_batch_type_task_id,
        batch_id_deprecated=True
    )


def variants(batch_id:str, analysis_ids: list, bioinfo_analysis_code: str, target_batch_types: List[ClinAnalysis], spark_jar: str, skip: str, detect_batch_type_task_id: str) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='variants',
        task_id='variants',
        name='etl-normalize-variants',
        steps='default',
        app_name='etl_normalize_variants',
        spark_class=NORMALIZED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        bioinfo_analysis_code=bioinfo_analysis_code,
        target_batch_types=target_batch_types,
        detect_batch_type_task_id=detect_batch_type_task_id,
        batch_id_deprecated=True
    )


def consequences(batch_id:str, analysis_ids: list, bioinfo_analysis_code: str, target_batch_types: List[ClinAnalysis], spark_jar: str, skip: str, detect_batch_type_task_id: str) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='consequences',
        task_id='consequences',
        name='etl-normalize-consequences',
        steps='default',
        app_name='etl_normalize_consequences',
        spark_class=NORMALIZED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        bioinfo_analysis_code=bioinfo_analysis_code,
        target_batch_types=target_batch_types,
        detect_batch_type_task_id=detect_batch_type_task_id,
        batch_id_deprecated=True
    )


def coverage_by_gene(batch_id: str, analysis_ids: list, bioinfo_analysis_code: str, target_batch_types: List[ClinAnalysis], spark_jar: str, skip: str, detect_batch_type_task_id: str) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='coverage_by_gene',
        task_id='coverage_by_gene',
        name='etl-normalize-coverage-by-gene',
        steps='default',
        app_name='etl_ingest_normalize_by_gene',
        spark_class=NORMALIZED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        bioinfo_analysis_code=bioinfo_analysis_code,
        target_batch_types=target_batch_types,
        detect_batch_type_task_id=detect_batch_type_task_id,
        batch_id_deprecated=True
    )


def exomiser(batch_id: str, analysis_ids: list, target_batch_types: List[ClinAnalysis], spark_jar: str, skip: str, detect_batch_type_task_id: str) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='exomiser',
        task_id='exomiser',
        name='etl-normalize-exomiser',
        steps='default',
        app_name='etl_normalize_exomiser',
        spark_class=NORMALIZED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        target_batch_types=target_batch_types,
        detect_batch_type_task_id=detect_batch_type_task_id,
        batch_id_deprecated=True
    )

def exomiser_cnv(batch_id: str, analysis_ids: list, target_batch_types: List[ClinAnalysis], spark_jar: str, skip: str, detect_batch_type_task_id: str) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='exomiser_cnv',
        task_id='exomiser_cnv',
        name='etl-normalize-exomiser',
        steps='default',
        app_name='etl_normalize_exomiser_cnv',
        spark_class=NORMALIZED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        target_batch_types=target_batch_types,
        batch_id_deprecated=True,
        detect_batch_type_task_id=detect_batch_type_task_id,
    )

def franklin(batch_id: str, analysis_ids: list, target_batch_types: List[ClinAnalysis], spark_jar: str, skip: str, detect_batch_type_task_id: str) -> SparkETLOperator:
    return SparkETLOperator(
        entrypoint='franklin',
        task_id='franklin',
        name='etl-normalize-franklin',
        steps='default',
        app_name='etl_normalize_franklin',
        spark_class=NORMALIZED_MAIN_CLASS,
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        skip=skip,
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        target_batch_types=target_batch_types,
        batch_id_deprecated=True,
        detect_batch_type_task_id=detect_batch_type_task_id,
    )
