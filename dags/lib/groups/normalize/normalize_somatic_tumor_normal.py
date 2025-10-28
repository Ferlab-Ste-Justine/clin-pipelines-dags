from airflow.decorators import task_group
from lib.tasks import normalize
from lib.utils_etl import BioinfoAnalysisCode, ClinAnalysis, skip


@task_group(group_id='normalize')
def normalize_somatic_tumor_normal(
        batch_id: str,
        analysis_ids: list,
        skip_all: str,
        skip_snv_somatic: str,
        skip_variants: str,
        skip_consequences: str,
        skip_coverage_by_gene: str,
        spark_jar: str,
        detect_batch_type_task_id: str = None,
):
    
    target_batch_types = [ClinAnalysis.SOMATIC_TUMOR_NORMAL]

    snv_somatic = normalize.snv_somatic(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_snv_somatic), detect_batch_type_task_id)
    variants = normalize.variants(batch_id, analysis_ids, BioinfoAnalysisCode.TNEBA.value, target_batch_types, spark_jar, skip(skip_all, skip_variants), detect_batch_type_task_id)
    consequences = normalize.consequences(batch_id, analysis_ids, BioinfoAnalysisCode.TNEBA.value, target_batch_types, spark_jar, skip(skip_all, skip_consequences), detect_batch_type_task_id)
    coverage_by_gene = normalize.coverage_by_gene(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_coverage_by_gene), detect_batch_type_task_id)

    snv_somatic >> variants >> consequences >> coverage_by_gene
