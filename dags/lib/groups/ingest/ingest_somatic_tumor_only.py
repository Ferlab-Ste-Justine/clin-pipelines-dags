from airflow.decorators import task_group

from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.groups.normalize.normalize_somatic_tumor_only import normalize_somatic_tumor_only
from lib.tasks import (batch_type)
from lib.utils_etl import ClinAnalysis


@task_group(group_id='ingest_somatic_tumor_only')
def ingest_somatic_tumor_only(
        batch_id: str,
        color: str,
        skip_import: str,
        skip_batch: str,
        skip_snv_somatic: str,
        skip_cnv_somatic_tumor_only: str,
        skip_variants: str,
        skip_consequences: str,
        skip_coverage_by_gene: str,
        spark_jar: str
):
    validate_batch_type_task = batch_type.validate(
        batch_id=batch_id,
        batch_type=ClinAnalysis.SOMATIC_TUMOR_ONLY
    )

    ingest_fhir_group = ingest_fhir(
        batch_id=batch_id,
        color=color,
        skip_import=skip_import,
        skip_batch=skip_batch,
        spark_jar=spark_jar
    )

    normalize_somatic_tumor_only_group = normalize_somatic_tumor_only(
        batch_id=batch_id,
        skip_snv_somatic=skip_snv_somatic,
        skip_cnv_somatic_tumor_only=skip_cnv_somatic_tumor_only,
        skip_variants=skip_variants,
        skip_consequences=skip_consequences,
        skip_coverage_by_gene=skip_coverage_by_gene,
        spark_jar=spark_jar
    )

    validate_batch_type_task >> ingest_fhir_group >> normalize_somatic_tumor_only_group
