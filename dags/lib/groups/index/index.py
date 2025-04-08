from airflow.decorators import task_group
from lib.tasks import index as index_tasks
from lib.utils_etl import release_id


@task_group(group_id='index')
def index(
        color: str,
        spark_jar: str,
        skip_cnv_centric: str = ''
):
    """
    Run all index tasks. CNV centric can be skipped for runs where CNVs are not indexed.
    """
    gene_centric = index_tasks.gene_centric(release_id('gene_centric'), color, spark_jar)
    gene_suggestions = index_tasks.gene_suggestions(release_id('gene_suggestions'), color, spark_jar)
    variant_centric = index_tasks.variant_centric(release_id('variant_centric'), color, spark_jar)
    variant_suggestions = index_tasks.variant_suggestions(release_id('variant_suggestions'), color, spark_jar)
    cnv_centric = index_tasks.cnv_centric(release_id('cnv_centric'), color, spark_jar, skip=skip_cnv_centric)
    coverage_by_gene_centric = index_tasks.coverage_by_gene_centric(release_id('coverage_by_gene_centric'), color, spark_jar)

    gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric >> coverage_by_gene_centric
