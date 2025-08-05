from typing import List, Optional, Set

from airflow.decorators import task
from lib.datasets import enriched_clinical
from pandas import DataFrame


@task.virtualenv(task_id='get_all_analysis_ids', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
def get_all_analysis_ids(analysis_ids: Optional[Set[str]] = None, sequencing_ids: Optional[Set[str]] = None, batch_ids: Optional[Set[str]] = None, batch_id: Optional[str] = None, skip: bool = False) -> List[str]:
    
    if skip:
        return []
    
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

@task.virtualenv(task_id='get_analysis_ids_related_batch', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
def get_analysis_ids_related_batch(analysis_ids: Optional[Set[str]], batch_id: str, skip: bool = False) -> str:

    '''
    Utils function to revert to Batch ID a list of analysis IDs provided to a DAG as param.
    Will probably be mostly used in etl_ingest as long as we have ETLs using batch_id as input.
    '''
    
    if batch_id and len(batch_id) > 0:
        return batch_id
    
    if skip:
        return ""
    
    if not analysis_ids or len(analysis_ids) == 0:
        raise AirflowFailException(f"No batch ID or analysis IDs were provided.")
    
    from airflow.exceptions import AirflowFailException
    from lib.datasets import enriched_clinical
    from lib.utils_etl_tables import get_batch_ids, to_pandas

    df: DataFrame = to_pandas(enriched_clinical.uri)
    clinical_df = df[["analysis_id", "sequencing_id", "batch_id"]]
    all_batch_ids = sorted(get_batch_ids(clinical_df, analysis_ids=analysis_ids))

    if not all_batch_ids or len(all_batch_ids) == 0:
        raise AirflowFailException(f"No batch IDs found for the provided analysis IDs ({analysis_ids}).")
    
    if len(all_batch_ids) > 1:
        raise AirflowFailException(f"Analysis IDs belong to more than one batch ID ({all_batch_ids}).")

    return list(all_batch_ids)[0]
