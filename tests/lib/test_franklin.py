import json
import time
from typing import List
from unittest.mock import patch

import pandas as pd
import pytest
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@pytest.fixture
def solo_analysis_row() -> dict:
    """
    Solo analysis submitted using legacy method (batch_id provided).
    """
    return {'analysis_id': 'A1', 'family_id': None, 'aliquot_id': '1', 'sequencing_id': 'S1', 'batch_id': 'BATCH_1',
            'is_proband': True, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'affected_status': True,
            'first_name': 'Jean', 'birth_date': '2000-01-01', 'gender': 'Male', 'clinical_signs': None}


@pytest.fixture
def trio_analysis_rows() -> List[dict]:
    """
    Trio analysis submitted using new method (dummy batch_id).
    """
    return [
        {'analysis_id': 'A2', 'family_id': 'FM1', 'aliquot_id': '2', 'sequencing_id': 'S2', 'batch_id': 'foobar',
         'is_proband': True, 'father_aliquot_id': '3', 'mother_aliquot_id': '4', 'affected_status': True,
         'first_name': 'Jean', 'birth_date': '2000-01-01', 'gender': 'Male', 'clinical_signs': [
            {'id': 'HP:0000001'}, {'id': 'HP:0000002'}
        ]},
        {'analysis_id': 'A2', 'family_id': 'FM1', 'aliquot_id': '3', 'sequencing_id': 'S3', 'batch_id': 'foobar',
         'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'affected_status': False,
         'first_name': 'Jeannot', 'birth_date': '1970-01-01', 'gender': 'Male', 'clinical_signs': None},
        {'analysis_id': 'A2', 'family_id': 'FM1', 'aliquot_id': '4', 'sequencing_id': 'S4', 'batch_id': 'foobar',
         'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'affected_status': True,
         'first_name': 'Jeanne', 'birth_date': '1975-01-01', 'gender': 'Female', 'clinical_signs': None}
    ]


@pytest.fixture
def duo_analysis_rows() -> List[dict]:
    """
    Duo analysis submitted using new method (dummy batch_id).
    """
    return [
        {'analysis_id': 'A3', 'family_id': 'FM2', 'aliquot_id': '5', 'sequencing_id': 'S5', 'batch_id': 'foobar',
         'is_proband': True, 'father_aliquot_id': None, 'mother_aliquot_id': 6, 'affected_status': True},
        {'analysis_id': 'A3', 'family_id': 'FM2', 'aliquot_id': '6', 'sequencing_id': 'S6', 'batch_id': 'foobar',
         'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'affected_status': False}
    ]


@pytest.fixture
def clinical_data(solo_analysis_row, trio_analysis_rows, duo_analysis_rows) -> List[dict]:
    return [
        solo_analysis_row,
        *trio_analysis_rows,
        *duo_analysis_rows
    ]


@pytest.fixture
def family_groups(solo_analysis_row, trio_analysis_rows, duo_analysis_rows) -> dict:
    return {
        'families': {
            'FM1': trio_analysis_rows,
            'FM2': duo_analysis_rows,
        },
        'no_family': [solo_analysis_row]
    }


@pytest.fixture
def load_vcfs():
    def _load_vcfs(s3: S3Hook, bucket: str, paths: List[str]):
        for path in paths:
            s3.load_string(f'VCF content for {path}', key=path, bucket_name=bucket, replace=True)

    return _load_vcfs


@pytest.fixture
def with_vcf(load_vcfs):
    def _with_vcf(analyses: List[dict], bucket: str, key: str):
        for analysis in analyses:
            analysis['vcf_bucket'] = bucket
            analysis['vcf_key'] = key

        return analyses

    return _with_vcf


def test_get_clinical_data(clinical_data, solo_analysis_row, trio_analysis_rows):
    """
    It should return the clinical data for the given analysis IDs as a list of dictionaries.
    """
    from lib.franklin import get_clinical_data

    mock_df = pd.DataFrame(clinical_data)
    with patch('lib.franklin.to_pandas', return_value=mock_df):
        analysis_ids = ['A1', 'A2']
        result: List[dict] = get_clinical_data(analysis_ids)

        assert isinstance(result, list)
        assert all(isinstance(row, dict) for row in result)

        assert len(result) == 4
        assert solo_analysis_row in result
        assert all(row in result for row in trio_analysis_rows)


def test_group_families_from_clinical_data(clinical_data, solo_analysis_row, trio_analysis_rows, duo_analysis_rows):
    """
    It should group analysis_ids by family_id from the clinical dataframe.
    """
    from lib.franklin import group_families_from_clinical_data

    family_groups, solo_analyses = group_families_from_clinical_data(clinical_data)
    print(family_groups)
    print(solo_analyses)
    assert len(solo_analyses) == 1
    assert solo_analyses[0] == solo_analysis_row

    assert len(family_groups) == 2
    assert family_groups['FM_FRK_0001'] == trio_analysis_rows
    assert family_groups['FM_FRK_0002'] == duo_analysis_rows


def test_filter_valid_families(trio_analysis_rows, duo_analysis_rows):
    """
    It should filter out families that have an unsupported family member type.
    """
    from lib.franklin import filter_valid_families

    family_groups = {
        # Supported
        'FM1': trio_analysis_rows,
        'FM2': duo_analysis_rows,

        # Unsupported (aliquot_id 9 is a sister)
        'FM3': [
            {'analysis_id': 'A4', 'family_id': 'FM_FRK_0003', 'aliquot_id': '7', 'sequencing_id': 'S7',
             'is_proband': True,
             'father_aliquot_id': None, 'mother_aliquot_id': 8},
            {'analysis_id': 'A4', 'family_id': 'FM_FRK_0003', 'aliquot_id': '8', 'sequencing_id': 'S8',
             'is_proband': False,
             'father_aliquot_id': None, 'mother_aliquot_id': None},
            {'analysis_id': 'A4', 'family_id': 'FM_FRK_0003', 'aliquot_id': '9', 'sequencing_id': 'S9',
             'is_proband': False,
             'father_aliquot_id': None, 'mother_aliquot_id': 8}  # sister
        ],
    }

    filtered_families = filter_valid_families(family_groups)

    assert len(filtered_families) == 2
    assert 'FM3' not in filtered_families


def test_extract_vcf_prefix_for_legacy():
    """
    It should extract the VCF prefix from the S3 legacy key.
    """
    from lib.franklin import extract_vcf_prefix

    key = 'BATCH_1/1111.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    prefix = extract_vcf_prefix(key)
    assert prefix == '1111'


def test_extract_vcf_prefix_for_nextflow():
    """
    It should extract the VCF prefix from the S3 nextflow key.
    """
    from lib.franklin import extract_vcf_prefix

    key = 'variants.1111.snv.norm.VEP.vcf.gz'
    prefix = extract_vcf_prefix(key)
    assert prefix == '1111'


def test_attach_vcf_to_analyses(clin_minio, load_vcfs, family_groups, solo_analysis_row, trio_analysis_rows,
                                duo_analysis_rows):
    """
    It should attach VCFs to analyses.
    """
    from lib.config import clin_import_bucket, clin_nextflow_bucket
    from lib.franklin import attach_vcf_to_analyses

    # Legacy VCFs
    solo_vcf = 'BATCH_1/1.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    legacy_paths = [
        solo_vcf,
        'BATCH_1/2.case.hard-filtered.formatted.norm.VEP.vcf.gz',
        'BATCH_2/42.case.hard-filtered.formatted.norm.VEP.vcf.gz',
    ]
    load_vcfs(clin_minio, clin_import_bucket, legacy_paths)

    # Nextflow VCFs
    a2_vcf = 'post_processing/output/ensemblvep/variants.A2.snv.norm.VEP.vcf.gz'
    a3_vcf = 'post_processing/output/ensemblvep/variants.A3.snv.norm.VEP.vcf.gz'
    nextflow_paths = [
        a2_vcf,
        a3_vcf,
    ]
    load_vcfs(clin_minio, clin_nextflow_bucket, nextflow_paths)

    analyses_with_vcfs = attach_vcf_to_analyses(clin_minio, family_groups)

    aliquot_id_vcf_map = {}
    for family in analyses_with_vcfs['families'].values():
        for f in family:
            aliquot_id_vcf_map[f['aliquot_id']] = {
                'bucket': f['vcf_bucket'],
                'key': f['vcf_key']
            }

    for a in analyses_with_vcfs['no_family']:
        aliquot_id_vcf_map[a['aliquot_id']] = {
            'bucket': a['vcf_bucket'],
            'key': a['vcf_key']
        }

    assert len(analyses_with_vcfs) == 2
    assert len(aliquot_id_vcf_map) == 6
    assert aliquot_id_vcf_map == {
        '1': {
            'bucket': clin_import_bucket,
            'key': solo_vcf
        },
        '2': {
            'bucket': clin_nextflow_bucket,
            'key': a2_vcf
        },
        '3': {
            'bucket': clin_nextflow_bucket,
            'key': a2_vcf
        },
        '4': {
            'bucket': clin_nextflow_bucket,
            'key': a2_vcf
        },
        '5': {
            'bucket': clin_nextflow_bucket,
            'key': a3_vcf
        },
        '6': {
            'bucket': clin_nextflow_bucket,
            'key': a3_vcf
        }
    }


def test_can_create_analysis_with_no_existing_analyses(clin_minio, duo_analysis_rows):
    """
    Case #1: It should return True if there are no existing Franklin analyses for the given analyses.
    """
    from lib.franklin import can_create_analysis

    # Case 1: No existing analyses
    assert can_create_analysis(clin_minio, duo_analysis_rows) is True


def test_can_create_analysis_with_completed_analyses(clin_minio, trio_analysis_rows):
    """
    It should return False if there are existing completed Franklin analyses for the given analyses.
    """
    from lib.franklin import can_create_analysis
    from lib.config import clin_datalake_bucket

    analysis_id = 'A2'
    aliquot_id = '2'

    # Load completed analysis for aliquot_id=2
    clin_minio.load_string("{}",
                           f'raw/landing/franklin/analysis_id={analysis_id}/aliquot_id={aliquot_id}/franklin_analysis_id=123/analysis.json',
                           bucket_name=clin_datalake_bucket, replace=True)

    assert can_create_analysis(clin_minio, trio_analysis_rows) is False


def test_can_create_analysis_with_in_progress_analyses(clin_minio, trio_analysis_rows):
    """
    It should return False if there are existing Franklin analyses in progress for the given analyses.
    """
    from lib.franklin import can_create_analysis
    from lib.config import clin_datalake_bucket

    analysis_id = 'A2'
    aliquot_id = '2'

    # Load in-progress analysis for aliquot_id=2
    clin_minio.load_string("CREATED",
                           f'raw/landing/franklin/analysis_id={analysis_id}/aliquot_id={aliquot_id}/_FRANKLIN_STATUS_.txt',
                           bucket_name=clin_datalake_bucket, replace=True)

    assert can_create_analysis(clin_minio, trio_analysis_rows) is False


def test_transfer_vcf_to_franklin(clin_minio, franklin_s3, load_vcfs, with_vcf, solo_analysis_row, trio_analysis_rows):
    """
    It should transfer VCF files from the Clin S3 bucket to the Franklin S3 bucket.
    """
    from lib.config import clin_import_bucket, env, s3_franklin_bucket, clin_nextflow_bucket
    from lib.franklin import transfer_vcf_to_franklin

    # Load test VCF files into the Clin S3 bucket
    solo_vcf = 'BATCH_1/1.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    solo_analyses = with_vcf([solo_analysis_row], clin_import_bucket, solo_vcf)
    load_vcfs(clin_minio, clin_import_bucket, [solo_vcf])

    trio_vcf = 'post_processing/output/ensemblvep/variants.A2.snv.norm.VEP.vcf.gz'
    trio_analyses = with_vcf(trio_analysis_rows, clin_nextflow_bucket, trio_vcf)
    load_vcfs(clin_minio, clin_nextflow_bucket, [trio_vcf])

    analyses = [*solo_analyses, *trio_analyses]
    transfer_vcf_to_franklin(clin_minio, franklin_s3, analyses)

    # Check that the VCFs were transferred correctly
    all_vcfs = [solo_vcf, trio_vcf]
    for vcf in all_vcfs:
        destination_key = f'{env}/{vcf}'
        franklin_s3_response = franklin_s3.read_key(destination_key, bucket_name=s3_franklin_bucket)
        assert franklin_s3_response == f'VCF content for {vcf}', f'Content mismatch for {vcf}'


def test_build_create_analysis_payload_for_family_with_hpo(clin_minio, franklin_s3, trio_analysis_rows, with_vcf,
                                                           load_vcfs):
    """
    It should build the payload for creating analyses in Franklin.
    """
    from lib.config import s3_franklin_bucket, franklin_assay_id, env
    from lib.franklin import build_create_analysis_payload

    family_id = 'FM1'
    vcf_key = 'post_processing/output/ensemblvep/variants.A2.snv.norm.VEP.vcf.gz'
    franklin_vcf_key = f'{env}/{vcf_key}'
    analyses = with_vcf(trio_analysis_rows, s3_franklin_bucket, vcf_key)

    # Load VCF into the Franklin S3 bucket
    load_vcfs(franklin_s3, s3_franklin_bucket, [franklin_vcf_key])

    payload = build_create_analysis_payload(family_id, analyses, franklin_s3)
    expected_payload = {
        'upload_specs': {
            "source": 'AWS',
            "details": {
                "bucket": s3_franklin_bucket,
                'root_folder': '/'
            }
        },
        'analyses': [
            {
                'assay_id': franklin_assay_id,
                'sample_data': {
                    'sample_name': f'2 - {family_id}',
                    'name_in_vcf': '2',
                    'aws_files': [
                        {
                            'key': franklin_vcf_key,
                            'type': 'VCF_SHORT',
                        }
                    ],
                    'tissue_type': 'Whole Blood',
                    'patient_details': {
                        'name': 'Jean',
                        'dob': '2000-01-01',
                        'sex': 'Male',
                    }
                }
            },
            {
                'assay_id': franklin_assay_id,
                'sample_data': {
                    'sample_name': f'3 - {family_id}',
                    'name_in_vcf': '3',
                    'aws_files': [
                        {
                            'key': franklin_vcf_key,
                            'type': 'VCF_SHORT',
                        }
                    ],
                    'tissue_type': 'Whole Blood',
                    'patient_details': {
                        'name': 'Jeannot',
                        'dob': '1970-01-01',
                        'sex': 'Male',
                    }
                }
            },
            {
                'assay_id': franklin_assay_id,
                'sample_data': {
                    'sample_name': f'4 - {family_id}',
                    'name_in_vcf': '4',
                    'aws_files': [
                        {
                            'key': franklin_vcf_key,
                            'type': 'VCF_SHORT',
                        }
                    ],
                    'tissue_type': 'Whole Blood',
                    'patient_details': {
                        'name': 'Jeanne',
                        'dob': '1975-01-01',
                        'sex': 'Female',
                    }
                }
            }
        ],
        'family_analyses': [
            {
                'case_name': f'family - {family_id}',
                'family_samples': [
                    {
                        'sample_name': f'2 - {family_id}',
                        'family_relation': 'proband',
                        'is_affected': True,
                    },
                    {
                        'sample_name': f'3 - {family_id}',
                        'family_relation': 'father',
                        'is_affected': False,
                    },
                    {
                        'sample_name': f'4 - {family_id}',
                        'family_relation': 'mother',
                        'is_affected': True,
                    }
                ],
                'phenotypes': ['HP:0000001', 'HP:0000002']
            }
        ],
        'family_analyses_creation_specs': {
            'create_family_single_analyses': 'true'
        }
    }

    assert payload == expected_payload


def test_build_create_analysis_payload_for_solo_without_hpo(clin_minio, franklin_s3, solo_analysis_row, with_vcf,
                                                            load_vcfs):
    """
    It should build the payload for creating a solo analysis in Franklin without HPO data.
    """
    from lib.config import env, s3_franklin_bucket, franklin_assay_id
    from lib.franklin import build_create_analysis_payload

    vcf_key = 'BATCH_1/1.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    franklin_vcf_key = f'{env}/{vcf_key}'
    analyses = with_vcf([solo_analysis_row], s3_franklin_bucket, vcf_key)

    # Load VCF into the Franklin S3 bucket
    load_vcfs(franklin_s3, s3_franklin_bucket, [franklin_vcf_key])

    payload = build_create_analysis_payload(None, analyses, franklin_s3)
    expected_payload = {
        'upload_specs': {
            "source": 'AWS',
            "details": {
                "bucket": s3_franklin_bucket,
                'root_folder': '/'
            }
        },
        'analyses': [
            {
                'assay_id': franklin_assay_id,
                'sample_data': {
                    'sample_name': f'1 - None',
                    'name_in_vcf': '1',
                    'aws_files': [
                        {
                            'key': franklin_vcf_key,
                            'type': 'VCF_SHORT',
                        }
                    ],
                    'tissue_type': 'Whole Blood',
                    'patient_details': {
                        'name': 'Jean',
                        'dob': '2000-01-01',
                        'sex': 'Male',
                    }
                }
            }
        ]
    }

    assert payload == expected_payload


@pytest.mark.vpn
def test_post_create_analysis(clin_minio, franklin_s3, trio_analysis_rows, load_vcfs, with_vcf):
    """
    It should post the analysis creation request to the Franklin API and return the created IDs.
    """
    from lib.config import env, s3_franklin_bucket
    from lib.franklin import get_franklin_token, post_create_analysis

    family_id = 'FM1'
    vcf_key = 'post_processing/output/ensemblvep/variants.A2.snv.norm.VEP.vcf.gz'
    franklin_vcf_key = f'{env}/{vcf_key}'
    analyses = with_vcf(trio_analysis_rows, s3_franklin_bucket, vcf_key)

    # Load VCF into the Franklin S3 bucket
    load_vcfs(franklin_s3, s3_franklin_bucket, [franklin_vcf_key])

    token = get_franklin_token()
    franklin_ids = post_create_analysis(family_id, analyses, token, franklin_s3)

    print(franklin_ids)
    assert len(franklin_ids) == 4  # 3 analyses + 1 family analysis


def test_write_s3_analyses_status_for_family_with_ids(clin_minio, trio_analysis_rows):
    """
    It should write all analysis statuses and ids to the S3 bucket for a family.
    """
    from lib.franklin import write_s3_analyses_status, FranklinStatus
    from lib.config import clin_datalake_bucket

    analysis_id = 'A2'
    status = FranklinStatus.CREATED
    franklin_ids = [11, 22, 33]

    write_s3_analyses_status(clin_minio, trio_analysis_rows, status, franklin_ids)

    # Check if all status files were created
    for analysis in trio_analysis_rows:
        aliquot_id = analysis['aliquot_id']
        status_key = f'raw/landing/franklin/analysis_id={analysis_id}/aliquot_id={aliquot_id}/_FRANKLIN_STATUS_.txt'
        assert clin_minio.read_key(status_key, bucket_name=clin_datalake_bucket) == status.name

    # Check if the ids file was created
    ids_file_key = f'raw/landing/franklin/analysis_id={analysis_id}/_FRANKLIN_IDS_.txt'
    assert clin_minio.read_key(ids_file_key, bucket_name=clin_datalake_bucket) == '11,22,33'


def test_write_s3_analysis_status_for_solo_with_id(clin_minio, solo_analysis_row):
    """
    It should write the analysis status and id to the S3 bucket for a solo analysis.
    """
    from lib.franklin import write_s3_analysis_status, FranklinStatus
    from lib.config import clin_datalake_bucket

    analysis_id = solo_analysis_row['analysis_id']
    aliquot_id = solo_analysis_row['aliquot_id']
    status = FranklinStatus.READY
    franklin_id = '11'

    write_s3_analysis_status(clin_minio, analysis_id, aliquot_id, status, id=franklin_id)

    # Check if the status file was created
    status_key = f'raw/landing/franklin/analysis_id={analysis_id}/aliquot_id={aliquot_id}/_FRANKLIN_STATUS_.txt'
    assert clin_minio.read_key(status_key, bucket_name=clin_datalake_bucket) == status.name

    # Check if the id file was created
    ids_file_key = f'raw/landing/franklin/analysis_id={analysis_id}/aliquot_id={aliquot_id}/_FRANKLIN_ID_.txt'
    assert clin_minio.read_key(ids_file_key, bucket_name=clin_datalake_bucket) == '11'


def test_extract_param_from_s3_key():
    """
    It should extract parameters from the S3 key.
    """
    from lib.franklin import extract_param_from_s3_key

    family_id = 'FM_FRK_0001'
    aliquot_id = '1'
    analysis_id = '11'
    key = f'raw/landing/franklin/batch_id=BATCH_1/family_id={family_id}/aliquot_id={aliquot_id}/analysis_id={analysis_id}/analysis.json'

    extracted_family_id = extract_param_from_s3_key(key, 'family_id')
    assert extracted_family_id == family_id

    extracted_aliquot_id = extract_param_from_s3_key(key, 'aliquot_id')
    assert extracted_aliquot_id == aliquot_id

    extracted_analysis_id = extract_param_from_s3_key(key, 'analysis_id')
    assert extracted_analysis_id == analysis_id


@pytest.mark.vpn
def test_get_franklin_token():
    """
    It should return a token for accessing the Franklin API.
    """
    from lib.franklin import get_franklin_token

    token = get_franklin_token()

    assert token
    assert token.startswith('fake_token_')


@pytest.mark.vpn
def test_get_analysis_status(clin_minio, franklin_s3):
    """
    It should return the status of the analysis from Franklin.
    """
    from lib.config import env, s3_franklin_bucket
    from lib.franklin import get_franklin_token, get_analysis_status, post_create_analysis

    batch_id = 'BATCH_1'
    family_id = 'FM_FRK_0001'
    vcf_key = f'{batch_id}/{family_id}.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    analyses = [
        {
            'labAliquotId': '1', 'vcf': vcf_key, 'patient': {
            'familyMember': 'PROBAND', 'status': 'AFF', 'firstName': 'Jean', 'birthDate': '01/01/2000',
            'sex': 'male'},
        },
        {
            'labAliquotId': '2', 'vcf': vcf_key, 'patient': {
            'familyMember': 'MTH', 'status': 'UNF', 'firstName': 'Jeanne', 'birthDate': '01/01/1970',
            'sex': 'female'}
        },
    ]

    # Load VCF into the Franklin S3 bucket
    full_vcf_path = f'{env}/{vcf_key}'
    franklin_s3.load_string(f'VCF of family {family_id}', full_vcf_path, bucket_name=s3_franklin_bucket,
                            replace=True)

    token = get_franklin_token()
    franklin_ids = post_create_analysis(family_id, analyses, token, clin_minio, franklin_s3, batch_id)
    statuses = get_analysis_status(franklin_ids, token)

    assert len(statuses) == 3  # 2 analyses + 1 family analysis
    for status in statuses:
        assert status['status'] == 'ACTIVE'
        assert status['processing_status'] in ['PROCESSING_ANNOTATION', 'READY']


@pytest.mark.vpn
def test_get_completed_analysis_not_ready(clin_minio, franklin_s3):
    """
    It should fail if the analysis is not ready.
    """
    from lib.config import env, s3_franklin_bucket
    from lib.franklin import get_franklin_token, get_completed_analysis, post_create_analysis

    aliquot_id = '1'
    batch_id = 'BATCH_1'
    vcf_key = f'{batch_id}/{aliquot_id}.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    analyses = [
        {
            'labAliquotId': aliquot_id, 'vcf': vcf_key, 'patient': {
            'familyMember': 'PROBAND', 'status': 'AFF', 'firstName': 'Jean', 'birthDate': '01/01/2000',
            'sex': 'male'},
        }
    ]

    # Load VCF into the Franklin S3 bucket
    full_vcf_path = f'{env}/{vcf_key}'
    franklin_s3.load_string(f'VCF of aliquot {aliquot_id}', full_vcf_path, bucket_name=s3_franklin_bucket,
                            replace=True)

    token = get_franklin_token()
    franklin_id = post_create_analysis(None, analyses, token, clin_minio, franklin_s3, batch_id)[0]

    with pytest.raises(AirflowFailException) as e:
        get_completed_analysis(franklin_id, token)
        assert str(e.value) == f'Error from Franklin API call: analysis not ready: {franklin_id}'


@pytest.mark.vpn
@pytest.mark.slow
def test_get_completed_analysis_ready(clin_minio, franklin_s3):
    """
    It should return the response if the analysis is ready.
    """
    from lib.config import env, s3_franklin_bucket
    from lib.franklin import get_franklin_token, get_analysis_status, get_completed_analysis, post_create_analysis

    aliquot_id = '1'
    batch_id = 'BATCH_1'
    vcf_key = f'{batch_id}/{aliquot_id}.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    analyses = [
        {
            'labAliquotId': aliquot_id, 'vcf': vcf_key, 'patient': {
            'familyMember': 'PROBAND', 'status': 'AFF', 'firstName': 'Jean', 'birthDate': '01/01/2000',
            'sex': 'male'},
        }
    ]

    # Load VCF into the Franklin S3 bucket
    full_vcf_path = f'{env}/{vcf_key}'
    franklin_s3.load_string(f'VCF of aliquot {aliquot_id}', full_vcf_path, bucket_name=s3_franklin_bucket,
                            replace=True)

    token = get_franklin_token()
    franklin_id = post_create_analysis(None, analyses, token, clin_minio, franklin_s3, batch_id)[0]

    # Poll for READY status
    timeout_seconds = 70
    polling_interval = 5
    start_time = time.time()

    while True:
        status = get_analysis_status([franklin_id], token)[0]
        if status['processing_status'] == 'READY':
            break

        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Analysis {franklin_id} did not become READY within {timeout_seconds} seconds.")

        time.sleep(polling_interval)

    response = get_completed_analysis(franklin_id, token)
    parsed_response = json.loads(response)
    assert len(parsed_response) == 1
    assert 'variant' in parsed_response[0]
