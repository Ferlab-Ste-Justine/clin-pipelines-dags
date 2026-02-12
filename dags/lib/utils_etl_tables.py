from typing import Collection, Optional, Set

from lib.utils_etl import BioinfoAnalysisCode

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


def get_batch_ids(clinical_df: DataFrame, bioinfo_analysis_code: Optional[BioinfoAnalysisCode] = None, analysis_ids: Optional[Collection[str]] = None, sequencing_ids: Optional[Collection[str]] = None, only_the_most_recent_batch_id: bool = False) -> Set[str]:
    """
        Return the set of batch ids corresponding to the provided sequencing ids or analysis ids from the
        enriched_clinical table. If bioinfo_analysis_code is None, returns batch ids for all analysis codes.
        If only_the_most_recent_batch_id is True, returns only the most recent batch_id based on sequencing_id.
    """
    if not analysis_ids:
        analysis_ids = []

    if not sequencing_ids:
        sequencing_ids = []

    if not bioinfo_analysis_code:
        # No filtering by bioinfo_analysis_code
        filtered_df = clinical_df.loc[
            clinical_df["analysis_id"].isin(analysis_ids) | clinical_df["sequencing_id"].isin(sequencing_ids)
        ]
    else:
        # Filter by bioinfo_analysis_code
        filtered_df = clinical_df.loc[
            (clinical_df["bioinfo_analysis_code"] == bioinfo_analysis_code.value) & (clinical_df["analysis_id"].isin(analysis_ids) | clinical_df["sequencing_id"].isin(sequencing_ids))
        ]
    
    if only_the_most_recent_batch_id and not filtered_df.empty:
        # Sort by sequencing_id (descending) to get the most recent one first
        # sequencing_id is a PostgreSQL auto-generated ID, so higher values are more recent
        filtered_df = filtered_df.sort_values(by="sequencing_id", ascending=False)
        # Take the first batch_id (most recent)
        return set([filtered_df.iloc[0]["batch_id"]])
    else:
        return set(filtered_df["batch_id"])
