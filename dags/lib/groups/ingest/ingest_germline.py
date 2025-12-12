from airflow.decorators import task_group
from lib.groups.ingest.ingest_fhir import ingest_fhir
from lib.groups.normalize.normalize_germline import normalize_germline
from lib.tasks import batch_type, clinical
from lib.utils_etl import BioinfoAnalysisCode, ClinAnalysis, skip


@task_group(group_id='ingest_germline')
def ingest_germline(
        batch_id: str,
        analysis_ids: list,
        batch_type_detected: bool,
        color: str,
        skip_import: str,
        skip_post_import: str,
        skip_snv: str,
        skip_cnv: str,
        skip_variants: str,
        skip_consequences: str,
        skip_exomiser: str,
        skip_exomiser_cnv: str,
        skip_coverage_by_gene: str,
        skip_franklin: str,
        skip_nextflow: str,
        skip_batch: str,
        spark_jar: str
):
    skip_all = batch_type.skip(ClinAnalysis.GERMLINE, batch_type_detected)

    validate_batch_type_task = batch_type.validate(
        batch_id=batch_id,
        analysis_ids=analysis_ids,
        batch_type=ClinAnalysis.GERMLINE,
        skip=skip_all
    )

    ingest_fhir_group = ingest_fhir(
        batch_ids=[batch_id],
        color=color,
        skip_all=skip_all,
        skip_import=skip_import,
        skip_post_import=skip_post_import,
        spark_jar=spark_jar
    )

    get_all_analysis_ids = clinical.get_all_analysis_ids(analysis_ids=analysis_ids, batch_id=batch_id, skip=skip_all)
    get_analysis_ids_related_batch_task = clinical.get_analysis_ids_related_batch(
        bioinfo_analysis_code=BioinfoAnalysisCode.GEBA,
        analysis_ids=get_all_analysis_ids,
        batch_id=batch_id,
        skip=skip(skip_all, skip_batch)
    )

    normalize_germline_group = normalize_germline(
        batch_id=get_analysis_ids_related_batch_task,
        analysis_ids=get_all_analysis_ids,
        skip_all=skip_all,
        skip_snv=skip_snv,
        skip_cnv=skip_cnv,
        skip_variants=skip_variants,
        skip_consequences=skip_consequences,
        skip_exomiser=skip_exomiser,
        skip_exomiser_cnv=skip_exomiser_cnv,
        skip_coverage_by_gene=skip_coverage_by_gene,
        skip_franklin=skip_franklin,
        skip_nextflow=skip_nextflow,
        spark_jar=spark_jar
    )

    validate_batch_type_task >> ingest_fhir_group >> get_all_analysis_ids >> get_analysis_ids_related_batch_task >> normalize_germline_group
