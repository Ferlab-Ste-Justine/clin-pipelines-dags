import json
from contextlib import contextmanager
from unittest.mock import patch

import pandas as pd
import pytest


@pytest.fixture
def nextflow_bucket(mock_airflow_variables):
    from lib.config_nextflow import nextflow_bucket
    return nextflow_bucket


@pytest.fixture
def prepare(mock_airflow_variables, clean_up_clin_minio):
    from dags.lib.tasks.nextflow.exomiser import prepare
    return prepare


@pytest.fixture
def mock_clinical(clin_minio):
    @contextmanager
    def _patch(clinical_df):
        with (
            patch('lib.utils_etl_tables.to_pandas', return_value=clinical_df),
            patch('airflow.providers.amazon.aws.hooks.s3.S3Hook', return_value=clin_minio)
        ):
            yield
    return _patch


def _sex_by_individual(analysis_id: str, bucket: str, clin_minio) -> dict:
    """Return {individualId: sex} from the uploaded phenopacket pedigree.

    Proto3 omits enum fields left at their default (UNKNOWN_SEX == 0), so a
    missing key means the individual's sex stayed UNKNOWN.
    """
    from lib.config_nextflow import nextflow_exomiser_input_key
    family = json.loads(clin_minio.read_key(nextflow_exomiser_input_key(analysis_id), bucket))
    return {p['individualId']: p.get('sex', 'UNKNOWN_SEX') for p in family['pedigree']['persons']}


def _proband(analysis_id, aliquot_id, gender, father, mother):
    return {
        'analysis_id': analysis_id, 'family_id': 'FAM1', 'aliquot_id': aliquot_id,
        'gender': gender, 'is_proband': True, 'clinical_signs': [],
        'father_aliquot_id': father, 'mother_aliquot_id': mother,
        'affected_status_code': 'affected',
    }


def _relative(analysis_id, aliquot_id, gender, affected='unknown'):
    return {
        'analysis_id': analysis_id, 'family_id': 'FAM1', 'aliquot_id': aliquot_id,
        'gender': gender, 'is_proband': False, 'clinical_signs': [],
        'father_aliquot_id': None, 'mother_aliquot_id': None,
        'affected_status_code': affected,
    }


def test_prepare_coerces_unknown_parent_sex(nextflow_bucket, prepare, clin_minio, mock_clinical):
    # Trio where both parents have UNKNOWN gender (the CLIN-6017 case that Exomiser rejects).
    clinical_df = pd.DataFrame([
        _proband('SRA1', 'C1', 'Male', father='F1', mother='M1'),
        _relative('SRA1', 'F1', 'unknown'),
        _relative('SRA1', 'M1', 'unknown'),
    ])

    with mock_clinical(clinical_df):
        prepare.function({'SRA1'}, skip='')

    sex = _sex_by_individual('SRA1', nextflow_bucket, clin_minio)
    assert sex['F1'] == 'MALE'      # father coerced from UNKNOWN
    assert sex['M1'] == 'FEMALE'    # mother coerced from UNKNOWN
    assert sex['C1'] == 'MALE'      # proband untouched


def test_prepare_keeps_definite_sex_and_unknown_non_parents(nextflow_bucket, prepare, clin_minio, mock_clinical):
    # F1 is a father with a definite (if inconsistent) Female gender -> must NOT be flipped.
    # S1 is an unknown-sex sibling (not referenced as a parent) -> must stay UNKNOWN.
    clinical_df = pd.DataFrame([
        _proband('SRA1', 'C1', 'unknown', father='F1', mother='M1'),
        _relative('SRA1', 'F1', 'Female'),
        _relative('SRA1', 'M1', 'unknown'),
        _relative('SRA1', 'S1', 'unknown'),
    ])

    with mock_clinical(clinical_df):
        prepare.function({'SRA1'}, skip='')

    sex = _sex_by_individual('SRA1', nextflow_bucket, clin_minio)
    assert sex['F1'] == 'FEMALE'          # definite value preserved, not coerced
    assert sex['M1'] == 'FEMALE'          # mother coerced from UNKNOWN
    assert sex['S1'] == 'UNKNOWN_SEX'     # non-parent unknown left as-is
    assert sex['C1'] == 'UNKNOWN_SEX'     # proband unknown left as-is
