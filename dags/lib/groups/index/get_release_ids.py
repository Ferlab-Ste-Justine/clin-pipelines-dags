from typing import Optional

from airflow.decorators import task_group

from lib.tasks import es


@task_group(group_id='get_release_ids')
def get_release_ids(
        release_id: str,
        color: str,
        gene_centric_release_id: Optional[str] = None,
        gene_suggestions_release_id: Optional[str] = None,
        variant_centric_release_id: Optional[str] = None,
        variant_suggestions_release_id: Optional[str] = None,
        coverage_by_gene_centric_release_id: Optional[str] = None,
        cnv_centric_release_id: Optional[str] = None,
        increment_release_id: bool = True,
        skip_cnv_centric: str = ''
):
    """
    Get release id for each index.

    If increment_release_id is True, returns a new id. Otherwise, returns the current id.
    """
    es.get_release_id \
        .override(task_id='get_gene_centric_release_id')(
            release_id=gene_centric_release_id if gene_centric_release_id else release_id,
            color=color,
            index='gene_centric',
            increment=increment_release_id
        )
    es.get_release_id \
        .override(task_id='get_gene_suggestions_release_id')(
            release_id=gene_suggestions_release_id if gene_suggestions_release_id else release_id,
            color=color,
            index='gene_suggestions',
            increment=increment_release_id
        )
    es.get_release_id \
        .override(task_id='get_variant_centric_release_id')(
            release_id=variant_centric_release_id if variant_centric_release_id else release_id,
            color=color,
            index='variant_centric',
            increment=increment_release_id
        )
    es.get_release_id \
        .override(task_id='get_variant_suggestions_release_id')(
            release_id=variant_suggestions_release_id if variant_suggestions_release_id else release_id,
            color=color,
            index='variant_suggestions',
            increment=increment_release_id
        )
    es.get_release_id \
        .override(task_id='get_coverage_by_gene_centric_release_id')(
            release_id=coverage_by_gene_centric_release_id if coverage_by_gene_centric_release_id else release_id,
            color=color,
            index='coverage_by_gene_centric',
            increment=increment_release_id
        )
    es.get_release_id \
        .override(task_id='get_cnv_centric_release_id')(
            release_id=cnv_centric_release_id if cnv_centric_release_id else release_id,
            color=color,
            index='cnv_centric',
            increment=increment_release_id,
            skip=skip_cnv_centric
        )
