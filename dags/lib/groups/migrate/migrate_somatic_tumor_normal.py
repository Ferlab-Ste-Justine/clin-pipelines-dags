from airflow.decorators import task_group

from lib.groups.normalize.normalize_somatic_tumor_normal import normalize_somatic_tumor_normal
from lib.tasks import batch_type
from lib.utils_etl import ClinAnalysis, get_group_id


@task_group(group_id='migrate_somatic_tumor_normal')
def migrate_somatic_tumor_normal(
        batch_id: str,
        skip_snv_somatic: str,
        skip_variants: str,
        skip_consequences: str,
        skip_coverage_by_gene: str,
        spark_jar: str
):
    detect_batch_type_task_id = f"{get_group_id('migrate', batch_id)}.detect_batch_type"
    skip_all = batch_type.skip(
        batch_type=ClinAnalysis.SOMATIC_TUMOR_NORMAL,
        batch_type_detected=True,
        detect_batch_type_task_id=detect_batch_type_task_id
    )

    validate_somatic_tumor_normal_task = batch_type.validate(
        batch_id=batch_id,
        batch_type=ClinAnalysis.SOMATIC_TUMOR_NORMAL,
        skip=skip_all
    )

    normalize_somatic_tumor_normal_group = normalize_somatic_tumor_normal(
        batch_id=batch_id,
        skip_all=skip_all,
        skip_snv_somatic=skip_snv_somatic,
        skip_variants=skip_variants,
        skip_consequences=skip_consequences,
        skip_coverage_by_gene=skip_coverage_by_gene,
        spark_jar=spark_jar
    )

    validate_somatic_tumor_normal_task >> normalize_somatic_tumor_normal_group
