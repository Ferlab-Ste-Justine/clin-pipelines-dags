import pandas as pd
import pytest


@pytest.fixture
def clinical_df():
    return pd.DataFrame([
        {'sequencing_id': 'SRS0001', 'batch_id': 'BAT1', 'analysis_id': 'SRA0001', 'bioinfo_analysis_code': 'TNEBA'},
        {'sequencing_id': 'SRS0002', 'batch_id': 'BAT2', 'analysis_id': 'SRA0001', 'bioinfo_analysis_code': 'TEBA'},
        {'sequencing_id': 'SRS0003', 'batch_id': 'BAT2', 'analysis_id': 'SRA0002', 'bioinfo_analysis_code': 'TEBA'},
        {'sequencing_id': 'SRS0004', 'batch_id': 'BAT3', 'analysis_id': 'SRA0003', 'bioinfo_analysis_code': 'GEBA'},
        {'sequencing_id': 'SRS0005', 'batch_id': 'BAT4', 'analysis_id': 'SRA0004', 'bioinfo_analysis_code': 'GEBA'},
    ])


def test_get_analysis_ids_from_sequencing_ids(clinical_df):
    from dags.lib.utils_etl_tables import get_analysis_ids
    analysis_ids = get_analysis_ids(clinical_df, sequencing_ids=['SRS0001', 'SRS0003'])

    assert analysis_ids == {'SRA0001', 'SRA0002'}


def test_get_analysis_ids_from_batch_ids(clinical_df):
    from dags.lib.utils_etl_tables import get_analysis_ids
    analysis_ids = get_analysis_ids(clinical_df, batch_ids=['BAT1', 'BAT3'])

    assert analysis_ids == {'SRA0001', 'SRA0003'}


def test_get_analysis_ids_from_sequencing_ids_and_batch_ids(clinical_df):
    from dags.lib.utils_etl_tables import get_analysis_ids
    analysis_ids = get_analysis_ids(clinical_df, sequencing_ids=['SRS0001', 'SRS0004'], batch_ids=['BAT2'])

    assert analysis_ids == {'SRA0001', 'SRA0002', 'SRA0003'}


def test_get_batch_ids_from_analysis_ids(clinical_df):
    from dags.lib.utils_etl_tables import get_batch_ids
    from dags.lib.utils_etl import BioinfoAnalysisCode
    batch_ids = get_batch_ids(clinical_df, bioinfo_analysis_code=BioinfoAnalysisCode.GEBA, analysis_ids=['SRA0003', 'SRA0004'])
    assert batch_ids == {'BAT3', 'BAT4'}


def test_get_batch_ids_from_sequencing_ids(clinical_df):
    from dags.lib.utils_etl_tables import get_batch_ids
    from dags.lib.utils_etl import BioinfoAnalysisCode
    batch_ids = get_batch_ids(clinical_df, bioinfo_analysis_code=BioinfoAnalysisCode.GEBA, sequencing_ids=['SRS0004', 'SRS0005'])
    assert batch_ids == {'BAT3', 'BAT4'}


def test_get_batch_ids_from_sequencing_ids_and_analysis_ids(clinical_df):
    from dags.lib.utils_etl_tables import get_batch_ids
    from dags.lib.utils_etl import BioinfoAnalysisCode
    batch_ids = get_batch_ids(clinical_df, bioinfo_analysis_code=BioinfoAnalysisCode.GEBA, sequencing_ids=['SRS0004'], analysis_ids=['SRA0004'])
    assert batch_ids == {'BAT3', 'BAT4'}


def test_get_batch_ids_from_analysis_id_in_both_tumor_only_and_tumor_normal_batches(clinical_df):
    from dags.lib.utils_etl_tables import get_batch_ids
    from dags.lib.utils_etl import BioinfoAnalysisCode
    batch_ids = get_batch_ids(clinical_df, bioinfo_analysis_code=BioinfoAnalysisCode.TEBA, analysis_ids=['SRA0001'])
    assert batch_ids == {'BAT2'}


def test_get_batch_ids_with_no_match(clinical_df):
    from dags.lib.utils_etl_tables import get_batch_ids
    from dags.lib.utils_etl import BioinfoAnalysisCode
    batch_ids = get_batch_ids(clinical_df, bioinfo_analysis_code=BioinfoAnalysisCode.GEBA, analysis_ids=['SRA0001'])
    assert batch_ids == set()
