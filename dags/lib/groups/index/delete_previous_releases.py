from airflow.decorators import task_group
from lib.slack import Slack
from lib.tasks import arranger, es
from lib.tasks import publish_index as publish


@task_group(group_id='delete_previous_releases')
def delete_previous_releases(
        gene_centric_release_id: str,
        gene_suggestions_release_id: str,
        variant_centric_release_id: str,
        variant_suggestions_release_id: str,
        cnv_centric_release_id: str,
        coverage_by_gene_centric_release_id: str,
        color: str,
        skip: str = ''
):
    delete_gene_centric = es.delete_previous_gene_centric_release(gene_centric_release_id, color, skip=skip)
    delete_gene_suggestions = es.delete_previous_gene_suggestions_release(gene_suggestions_release_id, color)
    delete_variant_centric = es.delete_previous_variant_centric_release(variant_centric_release_id, color, skip=skip)
    delete_variant_suggestions = es.delete_previous_variant_suggestions_release(variant_suggestions_release_id, color, skip=skip)
    delete_cnv_centric = es.delete_previous_cnv_centric_release(cnv_centric_release_id, color, skip=skip)
    delete_coverage_by_gene_centric = es.delete_previous_coverage_by_gene_centric_release(coverage_by_gene_centric_release_id, color, skip=skip)

    [delete_gene_centric, delete_gene_suggestions, delete_variant_centric, delete_variant_suggestions, delete_cnv_centric, delete_coverage_by_gene_centric]
