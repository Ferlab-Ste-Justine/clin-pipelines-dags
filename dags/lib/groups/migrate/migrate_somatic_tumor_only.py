from airflow.decorators import task_group
from lib.groups.normalize.normalize_somatic_tumor_only import \
    normalize_somatic_tumor_only
from lib.tasks import batch_type
from lib.utils_etl import ClinAnalysis, get_group_id


@task_group(group_id='migrate_somatic_tumor_only')
def migrate_somatic_tumor_only(
        batch_id: str,
        skip_snv_somatic: str,
        skip_cnv_somatic_tumor_only: str,
        skip_variants: str,
        skip_consequences: str,
        skip_coverage_by_gene: str,
        spark_jar: str
):
    skip_all = batch_type.skip(
        batch_type=ClinAnalysis.SOMATIC_TUMOR_ONLY,
        batch_type_detected=True,
    )

    validate_somatic_tumor_only_task = batch_type.validate(
        batch_id=batch_id,
        analysis_ids=[],
        batch_type=ClinAnalysis.SOMATIC_TUMOR_ONLY,
        skip=skip_all
    )

    normalize_somatic_tumor_only_group = normalize_somatic_tumor_only(
        batch_id=batch_id,
        analysis_ids=[],
        skip_all=skip_all,
        skip_snv_somatic=skip_snv_somatic,
        skip_cnv_somatic_tumor_only=skip_cnv_somatic_tumor_only,
        skip_variants=skip_variants,
        skip_consequences=skip_consequences,
        skip_coverage_by_gene=skip_coverage_by_gene,
        spark_jar=spark_jar
    )

    validate_somatic_tumor_only_task >> normalize_somatic_tumor_only_group
