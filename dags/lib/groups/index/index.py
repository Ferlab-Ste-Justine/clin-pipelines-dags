from airflow.decorators import task_group

from lib.tasks import index as index_tasks


@task_group(group_id='index')
def index(
        release_id: str,
        color: str,
        spark_jar: str,
):
    """
    Run all index tasks.
    """
    gene_centric = index_tasks.gene_centric(release_id, color, spark_jar)
    gene_suggestions = index_tasks.gene_suggestions(release_id, color, spark_jar)
    variant_centric = index_tasks.variant_centric(release_id, color, spark_jar)
    variant_suggestions = index_tasks.variant_suggestions(release_id, color, spark_jar)
    cnv_centric = index_tasks.cnv_centric(release_id, color, spark_jar)
    coverage_by_gene_centric = index_tasks.coverage_by_gene_centric(release_id, color, spark_jar)

    [gene_centric, gene_suggestions] >> variant_centric >> [variant_suggestions, cnv_centric, coverage_by_gene_centric]
