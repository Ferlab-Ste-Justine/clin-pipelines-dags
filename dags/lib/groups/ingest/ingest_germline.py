from airflow.decorators import task_group, task

from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.groups.normalize.normalize_germline import normalize_germline
from lib.tasks import (batch_type)
from lib.utils_etl import ClinAnalysis


@task_group(group_id='ingest_germline')
def ingest_germline(
        batch_id: str,
        batch_type_detected: bool,
        color: str,
        skip_import: str,
        skip_batch: str,
        skip_snv: str,
        skip_cnv: str,
        skip_variants: str,
        skip_consequences: str,
        skip_exomiser: str,
        skip_coverage_by_gene: str,
        skip_franklin: str,
        spark_jar: str
):
    skip_all = batch_type.skip(ClinAnalysis.GERMLINE, batch_type_detected)

    validate_batch_type_task = batch_type.validate(
        batch_id=batch_id,
        batch_type=ClinAnalysis.GERMLINE,
        skip=skip_all
    )

    ingest_fhir_group = ingest_fhir(
        batch_id=batch_id,
        color=color,
        skip_all=skip_all,
        skip_import=skip_import,
        skip_batch=skip_batch,
        spark_jar=spark_jar
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
        spark_jar=spark_jar
    )

    validate_batch_type_task >> ingest_fhir_group >> normalize_germline_group
