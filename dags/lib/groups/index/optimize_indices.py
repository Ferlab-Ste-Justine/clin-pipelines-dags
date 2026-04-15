from airflow.decorators import task_group
from lib.tasks import es_optimize


@task_group(group_id='optimize_indices')
def optimize_indices(
        gene_centric_release_id: str,
        gene_suggestions_release_id: str,
        variant_centric_release_id: str,
        variant_suggestions_release_id: str,
        cnv_centric_release_id: str,
        coverage_by_gene_centric_release_id: str,
        color: str,
        skip: str = ''
):
    indices = {
        'gene_centric': gene_centric_release_id,
        'gene_suggestions': gene_suggestions_release_id,
        'variant_centric': variant_centric_release_id,
        'variant_suggestions': variant_suggestions_release_id,
        'cnv_centric': cnv_centric_release_id,
        'coverage_by_gene_centric': coverage_by_gene_centric_release_id,
    }

    for name, rid in indices.items():
        wfg = es_optimize.wait_for_ready.override(task_id=f'wait_for_ready_{name}')(
            index_name=name, release_id=rid, color=color, skip=skip
        )
        fm = es_optimize.force_merge.override(task_id=f'force_merge_{name}')(
            index_name=name, release_id=rid, color=color, skip=skip
        )
        wfg >> fm
