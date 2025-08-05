from typing import Collection, Optional, Set

from pandas import DataFrame


def to_pandas(dataset_uri: str) -> DataFrame:
    """
        Load a dataset from Delta Lake into a pandas DataFrame.
    """
    from deltalake import DeltaTable
    from lib.config import s3_conn_id
    from lib.utils_s3 import get_s3_storage_options
    storage_options = get_s3_storage_options(s3_conn_id)

    dt: DeltaTable = DeltaTable(dataset_uri, storage_options=storage_options)
    return dt.to_pandas()


def get_analysis_ids(clinical_df: DataFrame, analysis_ids: Optional[Collection[str]] = None, sequencing_ids: Optional[Collection[str]] = None,
                     batch_ids: Optional[Collection[str]] = None) -> Set[str]:
    """
        Return the set of analysis ids corresponding to the provided sequencing ids or batch ids from the
        enriched_clinical table.
    """
    if not analysis_ids:
        analysis_ids = []

    if not sequencing_ids:
        sequencing_ids = []

    if not batch_ids:
        batch_ids = []

    return set(
        clinical_df.loc[
            clinical_df["analysis_id"].isin(analysis_ids) | clinical_df["sequencing_id"].isin(sequencing_ids) | clinical_df["batch_id"].isin(batch_ids),
            "analysis_id"
        ]
    )

def get_batch_ids(clinical_df: DataFrame, analysis_ids: Optional[Collection[str]] = None, sequencing_ids: Optional[Collection[str]] = None) -> Set[str]:
    """
        Return the set of batch ids corresponding to the provided sequencing ids or analysis ids from the
        enriched_clinical table.
    """
    if not analysis_ids:
        analysis_ids = []

    if not sequencing_ids:
        sequencing_ids = []

    return set(
        clinical_df.loc[
            clinical_df["analysis_id"].isin(analysis_ids) | clinical_df["sequencing_id"].isin(sequencing_ids),
            "batch_id"
        ]
    )