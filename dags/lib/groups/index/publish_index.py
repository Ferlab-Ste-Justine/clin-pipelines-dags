from airflow.decorators import task_group

from lib.slack import Slack
from lib.tasks import arranger
from lib.tasks import publish_index as publish


@task_group(group_id='publish')
def publish_index(
        release_id: str,
        color: str,
        spark_jar: str,
        skip_cnv_centric: str = ''
):
    """
    Run all publish tasks, remove project from arranger and restart arranger. CNV centric can be skipped for runs where
    CNVs are not indexed.
    """
    gene_centric = publish.gene_centric(release_id, color, spark_jar)
    gene_suggestions = publish.gene_suggestions(release_id, color, spark_jar)
    variant_centric = publish.variant_centric(release_id, color, spark_jar)
    variant_suggestions = publish.variant_suggestions(release_id, color, spark_jar)
    cnv_centric = publish.cnv_centric(release_id, color, spark_jar, skip=skip_cnv_centric)
    coverage_by_gene_centric = publish.coverage_by_gene_centric(release_id, color, spark_jar)

    arranger_remove_project_task = arranger.remove_project()
    arranger_restart_task = arranger.restart(on_success_callback=Slack.notify_dag_completion)

    [gene_centric, gene_suggestions, variant_centric, variant_suggestions, cnv_centric,
     coverage_by_gene_centric] >> arranger_remove_project_task >> arranger_restart_task
