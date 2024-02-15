from airflow.decorators import task_group

from lib.groups.normalize.normalize_germline import normalize_germline
from lib.tasks import batch_type
from lib.utils_etl import ClinAnalysis


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
        spark_jar: str
):
    validate_germline_task = batch_type.validate.override(task_id='validate_germline')(
        batch_id=batch_id,
        batch_type=ClinAnalysis.GERMLINE
    )

    normalize_germline_group = normalize_germline.override(group_id='normalize_germline')(
        batch_id=batch_id,
        skip_snv=skip_snv,
        skip_cnv=skip_cnv,
        skip_variants=skip_variants,
        skip_consequences=skip_consequences,
        skip_exomiser=skip_exomiser,
        skip_coverage_by_gene=skip_coverage_by_gene,
        skip_franklin=skip_franklin,
        spark_jar=spark_jar,
    )

    validate_germline_task >> normalize_germline_group
