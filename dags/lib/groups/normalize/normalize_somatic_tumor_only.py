from airflow.decorators import task_group
from lib.tasks import normalize
from lib.utils_etl import ClinAnalysis, skip


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
):
    target_batch_types = [ClinAnalysis.SOMATIC_TUMOR_ONLY]

    snv_somatic = normalize.snv_somatic(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_snv_somatic))
    cnv_somatic_tumor_only = normalize.cnv_somatic_tumor_only(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_cnv_somatic_tumor_only))
    variants = normalize.variants(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_variants))
    consequences = normalize.consequences(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_consequences))
    coverage_by_gene = normalize.coverage_by_gene(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_coverage_by_gene))

    snv_somatic >> cnv_somatic_tumor_only >> variants >> consequences >> coverage_by_gene
