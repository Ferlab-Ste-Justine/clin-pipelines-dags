from airflow.decorators import task_group

from lib.groups.franklin.franklin_update import FranklinUpdate
from lib.tasks import normalize, nextflow
from lib.utils_etl import skip


@task_group(group_id='normalize')
def normalize_germline(
        batch_id: str,
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
    snv = normalize.snv(batch_id, spark_jar, skip(skip_all, skip_snv))
    cnv = normalize.cnv(batch_id, spark_jar, skip(skip_all, skip_cnv))
    variants = normalize.variants(batch_id, spark_jar, skip(skip_all, skip_variants))
    consequences = normalize.consequences(batch_id, spark_jar, skip(skip_all, skip_consequences))
    exomiser = normalize.exomiser(batch_id, spark_jar, skip(skip_all, skip_exomiser))
    coverage_by_gene = normalize.coverage_by_gene(batch_id, spark_jar, skip(skip_all, skip_coverage_by_gene))

    franklin_update = FranklinUpdate(
        group_id='franklin_update',
        batch_id=batch_id,
        skip=skip(skip_all, skip_franklin),
        poke_interval=0,
        timeout=0,
    )

    franklin = normalize.franklin(batch_id, spark_jar, skip(skip_all, skip_franklin))

    @task_group(group_id="nextflow")
    def nextflow_group():
        prepare_svclustering_parental_origin_task = nextflow.prepare_svclustering_parental_origin(batch_id, spark_jar, skip(skip_all, skip_nextflow))
        run_svclustering_parental_origin = nextflow.svclustering_parental_origin(batch_id, skip(skip_all, skip_nextflow))
        normalize_svclustering_parental_origin_task = nextflow.normalize_svclustering_parental_origin(batch_id, spark_jar, skip(skip_all, skip_nextflow))

        prepare_svclustering_parental_origin_task >> run_svclustering_parental_origin >> normalize_svclustering_parental_origin_task

    snv >> cnv >> variants >> consequences >> exomiser >> coverage_by_gene >> franklin_update >> franklin >> nextflow_group()
