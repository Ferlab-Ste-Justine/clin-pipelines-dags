from airflow.decorators import task_group
from lib.groups.franklin.franklin_update import franklin_update
from lib.tasks import normalize
from lib.tasks.nextflow import svclustering_parental_origin
from lib.utils import check_task_output_exists
from lib.utils_etl import BioinfoAnalysisCode, ClinAnalysis, get_job_hash, skip


@task_group(group_id='normalize')
def normalize_germline(
        batch_id: str,
        analysis_ids: list,
        skip_all: str,
        skip_snv: str,
        skip_cnv: str,
        skip_variants: str,
        skip_consequences: str,
        skip_exomiser: str,
        skip_exomiser_cnv: str,
        skip_coverage_by_gene: str,
        skip_franklin: str,
        skip_nextflow: str,
        spark_jar: str,
        detect_batch_type_task_id: str = None,
):

    target_batch_types = [ClinAnalysis.GERMLINE]

    snv = normalize.snv(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_snv), detect_batch_type_task_id)
    cnv = normalize.cnv(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_cnv), detect_batch_type_task_id)
    variants = normalize.variants(batch_id, analysis_ids, BioinfoAnalysisCode.GEBA.value, target_batch_types, spark_jar, skip(skip_all, skip_variants), detect_batch_type_task_id)
    consequences = normalize.consequences(batch_id, analysis_ids, BioinfoAnalysisCode.GEBA.value, target_batch_types, spark_jar, skip(skip_all, skip_consequences), detect_batch_type_task_id)
    exomiser = normalize.exomiser(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_exomiser), detect_batch_type_task_id)
    exomiser_cnv = normalize.exomiser_cnv(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_exomiser_cnv), detect_batch_type_task_id)
    coverage_by_gene = normalize.coverage_by_gene(batch_id, analysis_ids, BioinfoAnalysisCode.GEBA.value, target_batch_types, spark_jar, skip(skip_all, skip_coverage_by_gene), detect_batch_type_task_id)

    franklin_update_task = franklin_update(
        analysis_ids=analysis_ids,
        skip=skip(skip_all, skip_franklin)
    )

    franklin = normalize.franklin(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_franklin), detect_batch_type_task_id)

    @task_group(group_id="nextflow")
    def nextflow_group():

        get_job_hash_task = get_job_hash(analysis_ids=analysis_ids, skip=skip(skip_all, skip_nextflow))

        prepare_svclustering_parental_origin_task = svclustering_parental_origin.prepare(analysis_ids, get_job_hash_task, skip(skip_all, skip_nextflow))
        check_should_run_svclustering_parental_origin = check_task_output_exists.override(task_id='check_should_run_svclustering_parental_origin')(prepare_svclustering_parental_origin_task, skip(skip_all, skip_nextflow))

        # Skipped if no input file
        run_svclustering_parental_origin = svclustering_parental_origin.run(get_job_hash_task, skip(skip_all, skip_nextflow))

        # Will still run if no input file but the normalization task is resilient
        normalize_svclustering_parental_origin_task = svclustering_parental_origin.normalize(batch_id, analysis_ids, spark_jar, skip(skip_all, skip_nextflow))

        (get_job_hash_task >> prepare_svclustering_parental_origin_task >> check_should_run_svclustering_parental_origin >>
         run_svclustering_parental_origin >> normalize_svclustering_parental_origin_task)

    snv >> cnv >> variants >> consequences >> exomiser >> exomiser_cnv >> coverage_by_gene >> franklin_update_task >> franklin >> nextflow_group()
