from airflow.decorators import task_group

from lib.groups.franklin.franklin_update import FranklinUpdate
from lib.tasks import normalize
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

    snv >> cnv >> variants >> consequences >> exomiser >> coverage_by_gene >> franklin_update >> franklin
