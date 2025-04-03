from pandas import DataFrame
from typing import Collection, Set


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


def get_analysis_ids(clinical_df: DataFrame, sequencing_ids: Collection[str]) -> Set[str]:
    """
        Return the set of analysis ids corresponding to the provided sequencing ids from the enriched_clinical table.
    """
    return set(
        clinical_df.loc[
            clinical_df["service_request_id"].isin(sequencing_ids),
            "analysis_service_request_id"
        ]
    )
