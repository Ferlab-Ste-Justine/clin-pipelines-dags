from airflow.decorators import task_group

from lib.groups.normalize.normalize_somatic_tumor_normal import normalize_somatic_tumor_normal
from lib.tasks import batch_type
from lib.utils_etl import ClinAnalysis


@task_group(group_id='migrate_somatic_tumor_normal')
def migrate_somatic_tumor_normal(
        batch_id: str,
        skip_snv_somatic: str,
        skip_variants: str,
        skip_consequences: str,
        skip_coverage_by_gene: str,
        spark_jar: str
):
    validate_somatic_tumor_normal_task = batch_type.validate.override(task_id='validate_somatic_tumor_normal')(
        batch_id=batch_id,
        batch_type=ClinAnalysis.SOMATIC_TUMOR_NORMAL
    )

    normalize_somatic_tumor_normal_group = normalize_somatic_tumor_normal.override(
        group_id='normalize_somatic_tumor_normal')(
        batch_id=batch_id,
        skip_snv_somatic=skip_snv_somatic,
        skip_variants=skip_variants,
        skip_consequences=skip_consequences,
        skip_coverage_by_gene=skip_coverage_by_gene,
        spark_jar=spark_jar
    )

    validate_somatic_tumor_normal_task >> normalize_somatic_tumor_normal_group
