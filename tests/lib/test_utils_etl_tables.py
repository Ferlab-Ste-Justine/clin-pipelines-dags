import pandas as pd
import pytest

from dags.lib.utils_etl_tables import get_analysis_ids


@pytest.fixture
def clinical_df():
    return pd.DataFrame([
        {'sequencing_id': 'SRS0001', 'batch_id': 'BAT1', 'analysis_id': 'SRA0001'},
        {'sequencing_id': 'SRS0002', 'batch_id': 'BAT2', 'analysis_id': 'SRA0001'},
        {'sequencing_id': 'SRS0003', 'batch_id': 'BAT2', 'analysis_id': 'SRA0002'},
        {'sequencing_id': 'SRS0004', 'batch_id': 'BAT3', 'analysis_id': 'SRA0003'},
    ])


def test_get_analysis_ids_from_sequencing_ids(clinical_df):
    analysis_ids = get_analysis_ids(clinical_df, sequencing_ids=['SRS0001', 'SRS0003'])

    assert analysis_ids == {'SRA0001', 'SRA0002'}


def test_get_analysis_ids_from_batch_ids(clinical_df):
    analysis_ids = get_analysis_ids(clinical_df, batch_ids=['BAT1', 'BAT3'])

    assert analysis_ids == {'SRA0001', 'SRA0003'}


def test_get_analysis_ids_from_sequencing_ids_and_batch_ids(clinical_df):
    analysis_ids = get_analysis_ids(clinical_df, sequencing_ids=['SRS0001', 'SRS0004'], batch_ids=['BAT2'])

    assert analysis_ids == {'SRA0001', 'SRA0002', 'SRA0003'}
