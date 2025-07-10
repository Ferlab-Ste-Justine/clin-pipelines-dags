from airflow.decorators import task_group
from lib import utils_nextflow
from lib.config_nextflow import (
    nextflow_bucket, nextflow_svclustering_parental_origin_input_key)
from lib.groups.franklin.franklin_update import FranklinUpdate
from lib.tasks import normalize
from lib.tasks.nextflow import svclustering_parental_origin
from lib.utils_etl import ClinAnalysis, skip


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
        skip_coverage_by_gene: str,
        skip_franklin: str,
        skip_nextflow: str,
        spark_jar: str,
):
    
    target_batch_types = [ClinAnalysis.GERMLINE]

    snv = normalize.snv(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_snv))
    cnv = normalize.cnv(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_cnv))
    variants = normalize.variants(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_variants))
    consequences = normalize.consequences(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_consequences))
    exomiser = normalize.exomiser(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_exomiser))
    coverage_by_gene = normalize.coverage_by_gene(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_coverage_by_gene))

    franklin_update = FranklinUpdate(
        group_id='franklin_update',
        batch_id=batch_id,
        skip=skip(skip_all, skip_franklin),
        poke_interval=0,
        timeout=0,
    )

    franklin = normalize.franklin(batch_id, analysis_ids, target_batch_types, spark_jar, skip(skip_all, skip_franklin))

    @task_group(group_id="nextflow")
    def nextflow_group():

        prepare_svclustering_parental_origin_task = svclustering_parental_origin.prepare(batch_id, spark_jar, skip(skip_all, skip_nextflow))
        check_svclustering_parental_origin_input_file_exists = utils_nextflow.check_input_file_exists.override(task_id='check_svclustering_parental_origin_input_file_exists')(
            bucket=nextflow_bucket,
            key=nextflow_svclustering_parental_origin_input_key(batch_id),
            skip=skip(skip_all, skip_nextflow))

        # Skipped if no input file
        run_svclustering_parental_origin = svclustering_parental_origin.run(batch_id, skip(skip_all, skip_nextflow))

        # Will still run if no input file but the normalization task is resilient
        normalize_svclustering_parental_origin_task = svclustering_parental_origin.normalize(batch_id, spark_jar, skip(skip_all, skip_nextflow))

        (prepare_svclustering_parental_origin_task >> check_svclustering_parental_origin_input_file_exists >>
         run_svclustering_parental_origin >> normalize_svclustering_parental_origin_task)

    snv >> cnv >> variants >> consequences >> exomiser >> coverage_by_gene >> franklin_update >> franklin >> nextflow_group()
