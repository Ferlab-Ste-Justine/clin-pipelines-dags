from typing import Set, List, Optional

from airflow.decorators import task
from pandas import DataFrame

from lib.datasets import enriched_clinical


@task.virtualenv(task_id='get_all_analysis_ids', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
def get_all_analysis_ids(sequencing_ids: Optional[Set[str]] = None, batch_ids: Optional[Set[str]] = None) -> List[str]:
    """
    Retrieves all analysis IDs for every sequencing IDs
    """
    from lib.datasets import enriched_clinical
    from lib.utils_etl_tables import get_analysis_ids, to_pandas

    df: DataFrame = to_pandas(enriched_clinical.uri)
    clinical_df = df[["sequencing_id", "analysis_id", "batch_id"]]

    return sorted(get_analysis_ids(clinical_df, sequencing_ids, batch_ids))
