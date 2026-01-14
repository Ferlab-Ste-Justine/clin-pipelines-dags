from airflow.decorators import task_group
from lib.tasks import normalize, vcf
from lib.utils_etl import BioinfoAnalysisCode, ClinAnalysis, skip


@task_group(group_id='normalize')
def normalize_somatic_tumor_only(
        batch_id: str,
        analysis_ids: list,
        skip_all: str,
        skip_snv_somatic: str,
        skip_cnv_somatic_tumor_only: str,
        skip_variants: str,
        skip_consequences: str,
        skip_coverage_by_gene: str,
        spark_jar: str,
        detect_batch_type_task_id: str = None,
):
    target_batch_types = [ClinAnalysis.SOMATIC_TUMOR_ONLY]

    convert_vcf_header_task = vcf.convert_vcf_header(analysis_ids, skip_all)
    snv_somatic = normalize.snv_somatic(batch_id, analysis_ids, BioinfoAnalysisCode.TEBA.value, target_batch_types, spark_jar, skip(skip_all, skip_snv_somatic), detect_batch_type_task_id)
    cnv_somatic_tumor_only = normalize.cnv_somatic_tumor_only(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_cnv_somatic_tumor_only), detect_batch_type_task_id)
    variants = normalize.variants(batch_id, analysis_ids, BioinfoAnalysisCode.TEBA.value, target_batch_types, spark_jar, skip(skip_all, skip_variants), detect_batch_type_task_id)
    consequences = normalize.consequences(batch_id, analysis_ids, BioinfoAnalysisCode.TEBA.value, target_batch_types, spark_jar, skip(skip_all, skip_consequences), detect_batch_type_task_id)
    coverage_by_gene = normalize.coverage_by_gene(batch_id, analysis_ids, BioinfoAnalysisCode.TEBA.value, target_batch_types, spark_jar, skip(skip_all, skip_coverage_by_gene), detect_batch_type_task_id)

    convert_vcf_header_task >> snv_somatic >> cnv_somatic_tumor_only >> variants >> consequences >> coverage_by_gene
