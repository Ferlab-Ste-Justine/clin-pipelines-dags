from airflow.decorators import task_group

from lib.groups.normalize.normalize_germline import normalize_germline
from lib.tasks import batch_type
from lib.utils_etl import ClinAnalysis, get_group_id


@task_group(group_id='migrate_germline')
def migrate_germline(
        batch_id: str,
        skip_snv: str,
        skip_cnv: str,
        skip_variants: str,
        skip_consequences: str,
        skip_exomiser: str,
        skip_coverage_by_gene: str,
        skip_franklin: str,
        skip_nextflow: str,
        spark_jar: str
):
    detect_batch_type_task_id = f"{get_group_id('migrate', batch_id)}.detect_batch_type"
    skip_all = batch_type.skip(
        batch_type=ClinAnalysis.GERMLINE,
        batch_type_detected=True,
        detect_batch_type_task_id=detect_batch_type_task_id
    )

    validate_germline_task = batch_type.validate(
        batch_id=batch_id,
        batch_type=ClinAnalysis.GERMLINE,
        skip=skip_all
    )

    normalize_germline_group = normalize_germline(
        batch_id=batch_id,
        skip_all=skip_all,
        skip_snv=skip_snv,
        skip_cnv=skip_cnv,
        skip_variants=skip_variants,
        skip_consequences=skip_consequences,
        skip_exomiser=skip_exomiser,
        skip_coverage_by_gene=skip_coverage_by_gene,
        skip_franklin=skip_franklin,
        skip_nextflow=skip_nextflow,
        spark_jar=spark_jar,
    )

    validate_germline_task >> normalize_germline_group
