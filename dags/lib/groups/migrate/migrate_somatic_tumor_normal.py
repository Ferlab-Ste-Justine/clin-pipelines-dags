from airflow.decorators import task_group
from lib.groups.normalize.normalize_somatic_tumor_normal import \
    normalize_somatic_tumor_normal
from lib.tasks import batch_type, clinical
from lib.utils_etl import BioinfoAnalysisCode, ClinAnalysis


@task_group(group_id='migrate_somatic_tumor_normal')
def migrate_somatic_tumor_normal(
        batch_id: str,
        skip_snv_somatic: str,
        skip_variants: str,
        skip_consequences: str,
        skip_coverage_by_gene: str,
        spark_jar: str
):
    skip_all = batch_type.skip(
        batch_type=ClinAnalysis.SOMATIC_TUMOR_NORMAL,
        batch_type_detected=True,
    )

    validate_somatic_tumor_normal_task = batch_type.validate(
        batch_id=batch_id,
        analysis_ids=[],
        batch_type=ClinAnalysis.SOMATIC_TUMOR_NORMAL,
        skip=skip_all
    )

    get_all_analysis_ids = clinical.get_all_analysis_ids(analysis_ids=[], batch_id=batch_id, skip=skip_all)
    get_analysis_ids_related_batch_task = clinical.get_analysis_ids_related_batch(bioinfo_analysis_code=BioinfoAnalysisCode.GEBA, analysis_ids=get_all_analysis_ids, batch_id=batch_id, skip=skip_all)

    normalize_somatic_tumor_normal_group = normalize_somatic_tumor_normal(
        batch_id=get_analysis_ids_related_batch_task,
        analysis_ids=get_all_analysis_ids,
        skip_all=skip_all,
        skip_snv_somatic=skip_snv_somatic,
        skip_variants=skip_variants,
        skip_consequences=skip_consequences,
        skip_coverage_by_gene=skip_coverage_by_gene,
        spark_jar=spark_jar
    )

    validate_somatic_tumor_normal_task >> normalize_somatic_tumor_normal_group
