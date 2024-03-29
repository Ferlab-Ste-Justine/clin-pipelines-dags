from airflow.decorators import task_group

from lib.tasks import prepare_index as prepare


@task_group(group_id='prepare')
def prepare_index(
        spark_jar: str,
        skip_cnv_centric: str = ''
):
    """
    Run all prepare index tasks. CNV centric can be skipped for runs where CNVs are not indexed.
    """
    gene_centric = prepare.gene_centric(spark_jar)
    gene_suggestions = prepare.gene_suggestions(spark_jar)
    variant_centric = prepare.variant_centric(spark_jar)
    variant_suggestions = prepare.variant_suggestions(spark_jar)
    cnv_centric = prepare.cnv_centric(spark_jar, skip=skip_cnv_centric)
    coverage_by_gene_centric = prepare.coverage_by_gene_centric(spark_jar)

    gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric >> coverage_by_gene_centric
