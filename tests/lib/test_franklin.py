import json
import time

import pytest
from airflow.exceptions import AirflowFailException


def test_group_families_from_metadata():
    """
    It should group families by types (SOLO, TRIO) from the metadata.
    """
    from lib.franklin import group_families_from_metadata

    metadata = {
        'analyses': [
            # SOLO
            {'patient': {'designFamily': 'SOLO', 'familyMember': 'PROBAND'}},
            # TRIO
            {'patient': {'designFamily': 'TRIO', 'familyMember': 'PROBAND', 'familyId': 'FM_FRK_0001'}},
            {'patient': {'designFamily': 'TRIO', 'familyMember': 'MTH', 'familyId': 'FM_FRK_0001'}},
            {'patient': {'designFamily': 'TRIO', 'familyMember': 'FTH', 'familyId': 'FM_FRK_0001'}}
        ]
    }

    [family_groups, analyses_without_family] = group_families_from_metadata(metadata)

    assert len(analyses_without_family) == 1
    assert len(family_groups) == 1
    assert len(family_groups['FM_FRK_0001']) == 3
    assert {p['patient']['familyMember'] for p in family_groups['FM_FRK_0001']} == {'PROBAND', 'MTH', 'FTH'}


def test_filter_valid_families():
    """
    It should filter out families that have an unsupported family member type.
    """
    from lib.franklin import filter_valid_families

    family_groups = {
        # DUOs
        'FM_FRK_0001': [{'patient': {'familyMember': 'PROBAND'}}, {'patient': {'familyMember': 'MTH'}}],
        'FM_FRK_0002': [{'patient': {'familyMember': 'PROBAND'}}, {'patient': {'familyMember': 'FTH'}}],

        # TRIO
        'FM_FRK_0003': [{'patient': {'familyMember': 'PROBAND'}}, {'patient': {'familyMember': 'MTH'}},
                        {'patient': {'familyMember': 'FTH'}}],

        # Unsupported
        'FM_FRK_0004': [{'patient': {'familyMember': 'PROBAND'}}, {'patient': {'familyMember': 'SIS'}}],
    }

    filtered_families = filter_valid_families(family_groups)

    assert len(filtered_families) == 3
    assert 'FM_FRK_0004' not in filtered_families


def test_transfer_vcf_to_franklin(clin_minio, franklin_s3):
    """
    It should transfer VCF files from the Clin S3 bucket to the Franklin S3 bucket.
    """
    from lib.config import clin_import_bucket, env, s3_franklin_bucket
    from lib.franklin import transfer_vcf_to_franklin

    analyses = [
        {'vcf': 'BATCH_1/1.case.hard-filtered.formatted.norm.VEP.vcf.gz', 'labAliquotId': '1'},
        {'vcf': 'BATCH_1/2.case.hard-filtered.formatted.norm.VEP.vcf.gz', 'labAliquotId': '2'},
        {'vcf': 'BATCH_1/3.case.hard-filtered.formatted.norm.VEP.vcf.gz', 'labAliquotId': '3'},
    ]

    # Load test VCF files into the Clin S3 bucket
    for analysis in analyses:
        vcf_key = analysis['vcf']
        aliquot_id = analysis['labAliquotId']
        clin_minio.load_string(f'VCF of aliquot ID {aliquot_id}', vcf_key, bucket_name=clin_import_bucket, replace=True)

    # First VCF already in Franklin S3 bucket
    franklin_s3.load_string(f'VCF of aliquot ID {analyses[0]['labAliquotId']}', analyses[0]['vcf'],
                            bucket_name=s3_franklin_bucket, replace=True)

    transfer_vcf_to_franklin(clin_minio, franklin_s3, analyses)

    # Check that the VCFs were transferred correctly
    for analysis in analyses:
        vcf_key = analysis['vcf']
        destination_key = f'{env}/{vcf_key}'
        franklin_s3_response = franklin_s3.read_key(destination_key, bucket_name=s3_franklin_bucket)
        assert franklin_s3_response == f'VCF of aliquot ID {analysis["labAliquotId"]}', f"Content mismatch for {vcf_key}"


def test_extract_vcf_prefix():
    """
    It should extract the VCF prefix from the S3 key.
    """
    from lib.franklin import extract_vcf_prefix

    key = 'BATCH_1/1111.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    prefix = extract_vcf_prefix(key)
    assert prefix == '1111'


def test_attach_vcf_to_analyses():
    """
    It should attach VCFs to analyses based on the family ID.
    """
    from lib.franklin import attach_vcf_to_analyses

    grouped_families = {
        'families': {
            # DUO 1
            'FM_FRK_0001': [{'labAliquotId': '1-a', 'patient': {'familyMember': 'PROBAND', 'familyId': 'FM_FRK_0001'}},
                            {'labAliquotId': '1-b', 'patient': {'familyMember': 'MTH', 'familyId': 'FM_FRK_0001'}}],
            # DUO 2
            'FM_FRK_0002': [{'labAliquotId': '2-a', 'patient': {'familyMember': 'PROBAND', 'familyId': 'FM_FRK_0002'}},
                            {'labAliquotId': '2-b', 'patient': {'familyMember': 'FTH', 'familyId': 'FM_FRK_0002'}}],
            # TRIO
            'FM_FRK_0003': [{'labAliquotId': '3-a', 'patient': {'familyMember': 'PROBAND', 'familyId': 'FM_FRK_0003'}},
                            {'labAliquotId': '3-b', 'patient': {'familyMember': 'MTH', 'familyId': 'FM_FRK_0003'}},
                            {'labAliquotId': '3-c', 'patient': {'familyMember': 'FTH', 'familyId': 'FM_FRK_0003'}}],
        },
        # SOLO
        'no_family': [
            {'labAliquotId': '4-a', 'patient': {'familyMember': 'PROBAND'}},
        ]
    }

    vcfs = {
        # DUO 1: one vcf per aliquotId
        'BATCH_1/1-a.case.hard-filtered.formatted.norm.VEP.vcf.gz': '1-a',
        'BATCH_1/1-b.case.hard-filtered.formatted.norm.VEP.vcf.gz': '1-b',
        # DUO 2: vcf for proband only
        'BATCH_1/2-a.case.hard-filtered.formatted.norm.VEP.vcf.gz': '2-a',
        # TRIO: vcf matches familyId
        'BATCH_1/FM_FRK_0003.case.hard-filtered.formatted.norm.VEP.vcf.gz': 'FM_FRK_0003',
        # SOLO: vcf for proband only
        'BATCH_1/4-a.case.hard-filtered.formatted.norm.VEP.vcf.gz': '4-a',
    }

    analyses_with_vcfs = attach_vcf_to_analyses(grouped_families, vcfs)
    aliquot_id_vcf_map = {}
    for family in analyses_with_vcfs['families'].values():
        for f in family:
            aliquot_id_vcf_map[f['labAliquotId']] = f['vcf']

    for a in analyses_with_vcfs['no_family']:
        aliquot_id_vcf_map[a['labAliquotId']] = a['vcf']

    assert len(analyses_with_vcfs) == 2
    assert len(aliquot_id_vcf_map) == 8
    assert aliquot_id_vcf_map == {
        '1-a': 'BATCH_1/1-a.case.hard-filtered.formatted.norm.VEP.vcf.gz',
        '1-b': 'BATCH_1/1-b.case.hard-filtered.formatted.norm.VEP.vcf.gz',
        '2-a': 'BATCH_1/2-a.case.hard-filtered.formatted.norm.VEP.vcf.gz',
        '2-b': 'BATCH_1/2-a.case.hard-filtered.formatted.norm.VEP.vcf.gz',  # proband vcf
        '3-a': 'BATCH_1/FM_FRK_0003.case.hard-filtered.formatted.norm.VEP.vcf.gz',
        '3-b': 'BATCH_1/FM_FRK_0003.case.hard-filtered.formatted.norm.VEP.vcf.gz',
        '3-c': 'BATCH_1/FM_FRK_0003.case.hard-filtered.formatted.norm.VEP.vcf.gz',
        '4-a': 'BATCH_1/4-a.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    }


def test_can_create_analysis_with_no_existing_analyses(clin_minio):
    """
    Case #1: It should return True if there are no existing analyses for the given batch and family.
    """
    from lib.franklin import can_create_analysis

    batch_id = 'BATCH_1'
    family_id = 'FM_FRK_0001'
    analyses = [{'labAliquotId': '1'}, {'labAliquotId': '1'}]

    # Case 1: No existing analyses
    assert can_create_analysis(clin_minio, batch_id, family_id, analyses) is True


def test_can_create_analysis_with_completed_analyses(clin_minio):
    """
    It should return False if there are existing completed analyses for the given batch and family.
    """
    from lib.franklin import can_create_analysis
    from lib.config import clin_datalake_bucket
    batch_id = 'BATCH_1'
    family_id = 'FM_FRK_0001'
    analyses = [{'labAliquotId': '1'}, {'labAliquotId': '2'}]

    # Load completed analysis for aliquot_id=2
    clin_minio.load_string("{}",
                           f'raw/landing/franklin/batch_id={batch_id}/family_id={family_id}/aliquot_id=2/analysis_id=2/analysis.json',
                           bucket_name=clin_datalake_bucket, replace=True)

    assert can_create_analysis(clin_minio, batch_id, family_id, analyses) is False


def test_can_create_analysis_with_in_progress_analyses(clin_minio):
    """
    It should return False if there are existing analyses in progress for the given batch and family.
    """
    from lib.franklin import can_create_analysis
    from lib.config import clin_datalake_bucket

    batch_id = 'BATCH_1'
    family_id = 'FM_FRK_0001'
    analyses = [{'labAliquotId': '1'}, {'labAliquotId': '2'}]

    # Load in-progress analysis for aliquot_id=2
    clin_minio.load_string("CREATED",
                           f'raw/landing/franklin/batch_id={batch_id}/family_id={family_id}/aliquot_id=2/_FRANKLIN_STATUS_.txt',
                           bucket_name=clin_datalake_bucket, replace=True)

    assert can_create_analysis(clin_minio, batch_id, family_id, analyses) is False


def test_write_s3_analyses_status_for_family_with_ids(clin_minio):
    """
    It should write all analysis statuses and ids to the S3 bucket for a family.
    """
    from lib.franklin import write_s3_analyses_status, FranklinStatus
    from lib.config import clin_datalake_bucket

    batch_id = 'BATCH_1'
    family_id = 'FM_FRK_0001'
    analyses = [{'labAliquotId': '1'}, {'labAliquotId': '2'}, {'labAliquotId': '3'}]
    status = FranklinStatus.CREATED
    franklin_ids = [11, 22, 33]

    write_s3_analyses_status(clin_minio, batch_id, family_id, analyses, status, franklin_ids)

    # Check if all status files were created
    for analysis in analyses:
        aliquot_id = analysis['labAliquotId']
        status_key = f'raw/landing/franklin/batch_id={batch_id}/family_id={family_id}/aliquot_id={aliquot_id}/_FRANKLIN_STATUS_.txt'
        assert clin_minio.read_key(status_key, bucket_name=clin_datalake_bucket) == status.name

    # Check if the ids file was created
    ids_file_key = f'raw/landing/franklin/batch_id={batch_id}/family_id={family_id}/_FRANKLIN_IDS_.txt'
    assert clin_minio.read_key(ids_file_key, bucket_name=clin_datalake_bucket) == '11,22,33'


def test_write_s3_analysis_status_for_solo_with_id(clin_minio):
    """
    It should write the analysis status and id to the S3 bucket for a solo analysis.
    """
    from lib.franklin import write_s3_analysis_status, FranklinStatus
    from lib.config import clin_datalake_bucket

    batch_id = 'BATCH_1'
    family_id = None  # Solo analysis has no family ID
    aliquot_id = '1'
    status = FranklinStatus.READY
    franklin_id = '11'

    write_s3_analysis_status(clin_minio, batch_id, family_id, aliquot_id, status, id=franklin_id)

    # Check if the status file was created
    status_key = f'raw/landing/franklin/batch_id={batch_id}/family_id=null/aliquot_id=1/_FRANKLIN_STATUS_.txt'
    assert clin_minio.read_key(status_key, bucket_name=clin_datalake_bucket) == status.name

    # Check if the id file was created
    ids_file_key = f'raw/landing/franklin/batch_id={batch_id}/family_id=null/aliquot_id=1/_FRANKLIN_ID_.txt'
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


def test_build_create_analysis_payload_for_family_with_hpo(clin_minio, franklin_s3):
    """
    It should build the payload for creating analyses in Franklin.
    """
    from lib.config import env, s3_franklin_bucket, franklin_assay_id, clin_import_bucket
    from lib.franklin import build_create_analysis_payload

    family_id = 'FM_FRK_0001'
    batch_id = 'BATCH_1'
    vcf_key = f'{batch_id}/{family_id}.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    analyses = [
        {
            'labAliquotId': '1', 'vcf': vcf_key, 'patient': {
            'familyMember': 'PROBAND', 'status': 'AFF', 'firstName': 'Jean', 'birthDate': '01/01/2000', 'sex': 'male'},
        },
        {
            'labAliquotId': '2', 'vcf': vcf_key, 'patient': {
            'familyMember': 'MTH', 'status': 'UNF', 'firstName': 'Jeanne', 'birthDate': '01/01/1970', 'sex': 'female'}
        },
    ]

    # Load VCF into the Franklin S3 bucket
    full_vcf_path = f'{env}/{vcf_key}'
    franklin_s3.load_string(f'VCF of family {family_id}', full_vcf_path, bucket_name=s3_franklin_bucket, replace=True)

    # Load HPO file into the Clin Minio bucket
    hpo_file_path = f'{batch_id}/1.hpo'  # batch_id/proband_id.hpo
    clin_minio.load_string("['HP:0000001', 'HP:0000002']", hpo_file_path, bucket_name=clin_import_bucket)

    payload = build_create_analysis_payload(family_id, analyses, batch_id, clin_minio, franklin_s3)
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
                    'sample_name': f'1 - {family_id}',
                    'name_in_vcf': '1',
                    'aws_files': [
                        {
                            'key': full_vcf_path,
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
                    'sample_name': f'2 - {family_id}',
                    'name_in_vcf': '2',
                    'aws_files': [
                        {
                            'key': full_vcf_path,
                            'type': 'VCF_SHORT',
                        }
                    ],
                    'tissue_type': 'Whole Blood',
                    'patient_details': {
                        'name': 'Jeanne',
                        'dob': '1970-01-01',
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
                        'sample_name': f'1 - {family_id}',
                        'family_relation': 'proband',
                        'is_affected': True,
                    },
                    {
                        'sample_name': f'2 - {family_id}',
                        'family_relation': 'mother',
                        'is_affected': False,
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


def test_build_create_analysis_payload_for_solo_without_hpo(clin_minio, franklin_s3):
    """
    It should build the payload for creating a solo analysis in Franklin without HPO data.
    """
    from lib.config import env, s3_franklin_bucket, franklin_assay_id
    from lib.franklin import build_create_analysis_payload

    aliquot_id = '1'
    batch_id = 'BATCH_1'
    vcf_key = f'{batch_id}/{aliquot_id}.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    analyses = [
        {
            'labAliquotId': aliquot_id, 'vcf': vcf_key, 'patient': {
            'familyMember': 'PROBAND', 'status': 'AFF', 'firstName': 'Jean', 'birthDate': '01/01/2000', 'sex': 'male'},
        }
    ]

    # Load VCF into the Franklin S3 bucket
    full_vcf_path = f'{env}/{vcf_key}'
    franklin_s3.load_string(f'VCF of aliquot {aliquot_id}', full_vcf_path, bucket_name=s3_franklin_bucket, replace=True)

    payload = build_create_analysis_payload(None, analyses, batch_id, clin_minio, franklin_s3)
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
                            'key': full_vcf_path,
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
def test_get_franklin_token():
    """
    It should return a token for accessing the Franklin API.
    """
    from lib.franklin import get_franklin_token

    token = get_franklin_token()

    assert token
    assert token.startswith('fake_token_')


@pytest.mark.vpn
def test_post_create_analysis(clin_minio, franklin_s3):
    """
    It should post the analysis creation request to the Franklin API and return the created IDs.
    """
    from lib.config import env, s3_franklin_bucket
    from lib.franklin import get_franklin_token, post_create_analysis

    batch_id = 'BATCH_1'
    family_id = 'FM_FRK_0001'
    vcf_key = f'{batch_id}/{family_id}.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    analyses = [
        {
            'labAliquotId': '1', 'vcf': vcf_key, 'patient': {
            'familyMember': 'PROBAND', 'status': 'AFF', 'firstName': 'Jean', 'birthDate': '01/01/2000', 'sex': 'male'},
        },
        {
            'labAliquotId': '2', 'vcf': vcf_key, 'patient': {
            'familyMember': 'MTH', 'status': 'UNF', 'firstName': 'Jeanne', 'birthDate': '01/01/1970', 'sex': 'female'}
        },
    ]

    # Load VCF into the Franklin S3 bucket
    full_vcf_path = f'{env}/{vcf_key}'
    franklin_s3.load_string(f'VCF of family {family_id}', full_vcf_path, bucket_name=s3_franklin_bucket, replace=True)

    token = get_franklin_token()
    franklin_ids = post_create_analysis(family_id, analyses, token, clin_minio, franklin_s3, batch_id)

    print(franklin_ids)
    assert len(franklin_ids) == 3  # 2 analyses + 1 family analysis


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
            'familyMember': 'PROBAND', 'status': 'AFF', 'firstName': 'Jean', 'birthDate': '01/01/2000', 'sex': 'male'},
        },
        {
            'labAliquotId': '2', 'vcf': vcf_key, 'patient': {
            'familyMember': 'MTH', 'status': 'UNF', 'firstName': 'Jeanne', 'birthDate': '01/01/1970', 'sex': 'female'}
        },
    ]

    # Load VCF into the Franklin S3 bucket
    full_vcf_path = f'{env}/{vcf_key}'
    franklin_s3.load_string(f'VCF of family {family_id}', full_vcf_path, bucket_name=s3_franklin_bucket, replace=True)

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
            'familyMember': 'PROBAND', 'status': 'AFF', 'firstName': 'Jean', 'birthDate': '01/01/2000', 'sex': 'male'},
        }
    ]

    # Load VCF into the Franklin S3 bucket
    full_vcf_path = f'{env}/{vcf_key}'
    franklin_s3.load_string(f'VCF of aliquot {aliquot_id}', full_vcf_path, bucket_name=s3_franklin_bucket, replace=True)

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
            'familyMember': 'PROBAND', 'status': 'AFF', 'firstName': 'Jean', 'birthDate': '01/01/2000', 'sex': 'male'},
        }
    ]

    # Load VCF into the Franklin S3 bucket
    full_vcf_path = f'{env}/{vcf_key}'
    franklin_s3.load_string(f'VCF of aliquot {aliquot_id}', full_vcf_path, bucket_name=s3_franklin_bucket, replace=True)

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
