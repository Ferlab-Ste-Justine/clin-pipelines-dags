from typing import List, Optional, Set

from airflow.decorators import task
from lib.datasets import enriched_clinical
from pandas import DataFrame


@task.virtualenv(task_id='get_all_analysis_ids', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
def get_all_analysis_ids(analysis_ids: Optional[Set[str]] = None, sequencing_ids: Optional[Set[str]] = None, batch_ids: Optional[Set[str]] = None, batch_id: Optional[str] = None) -> List[str]:
    """
    Retrieves all analysis IDs for every sequencing IDs
    """
    from airflow.exceptions import AirflowFailException
    from lib.datasets import enriched_clinical
    from lib.utils_etl_tables import get_analysis_ids, to_pandas

    batch_ids = batch_ids if batch_ids else [batch_id] if batch_id and batch_id != "" else []

    df: DataFrame = to_pandas(enriched_clinical.uri)
    clinical_df = df[["sequencing_id", "analysis_id", "batch_id"]]
    all_analysis_ids = sorted(get_analysis_ids(clinical_df, analysis_ids, sequencing_ids, batch_ids))

    if not all_analysis_ids:
        raise AirflowFailException(f"No analysis IDs found for the provided analysis IDs ({analysis_ids}), sequencing IDs ({sequencing_ids}) or batch IDs ({batch_ids}).")

    return all_analysis_ids
