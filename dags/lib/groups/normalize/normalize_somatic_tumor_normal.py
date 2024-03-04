from airflow.decorators import task_group

from lib.tasks import normalize
from lib.utils_etl import skip


@task_group(group_id='normalize')
def normalize_somatic_tumor_normal(
        batch_id: str,
        skip_all: str,
        skip_snv_somatic: str,
        skip_variants: str,
        skip_consequences: str,
        skip_coverage_by_gene: str,
        spark_jar: str,
):
    snv_somatic = normalize.snv_somatic(batch_id, spark_jar, skip(skip_all, skip_snv_somatic))
    variants = normalize.variants(batch_id, spark_jar, skip(skip_all, skip_variants))
    consequences = normalize.consequences(batch_id, spark_jar, skip(skip_all, skip_consequences))
    coverage_by_gene = normalize.coverage_by_gene(batch_id, spark_jar, skip(skip_all, skip_coverage_by_gene))

    snv_somatic >> variants >> consequences >> coverage_by_gene
