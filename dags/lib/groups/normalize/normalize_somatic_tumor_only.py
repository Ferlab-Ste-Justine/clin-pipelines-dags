from airflow.decorators import task_group

from lib.tasks import normalize


@task_group(group_id='normalize')
def normalize_somatic_tumor_only(
        batch_id: str,
        skip_snv_somatic: str,
        skip_cnv_somatic_tumor_only: str,
        skip_variants: str,
        skip_consequences: str,
        skip_coverage_by_gene: str,
        spark_jar: str,
):
    snv_somatic = normalize.snv_somatic(batch_id, spark_jar, skip_snv_somatic)
    cnv_somatic_tumor_only = normalize.cnv_somatic_tumor_only(batch_id, spark_jar, skip_cnv_somatic_tumor_only)
    variants = normalize.variants(batch_id, spark_jar, skip_variants)
    consequences = normalize.consequences(batch_id, spark_jar, skip_consequences)
    coverage_by_gene = normalize.coverage_by_gene(batch_id, spark_jar, skip_coverage_by_gene)

    snv_somatic >> cnv_somatic_tumor_only >> variants >> consequences >> coverage_by_gene
