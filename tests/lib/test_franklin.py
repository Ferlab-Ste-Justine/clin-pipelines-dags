import json
import time
from typing import List
from unittest.mock import patch

import pandas as pd
import pytest
from airflow.exceptions import AirflowFailException


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
def with_vcf():
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
    assert len(solo_analyses) == 1
    assert solo_analyses[0] == solo_analysis_row

    assert len(family_groups) == 2
    assert family_groups['FM1'] == trio_analysis_rows
    assert family_groups['FM2'] == duo_analysis_rows


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
    from lib.franklin import can_create_analysis, build_s3_analyses_json_key
    from lib.config import clin_datalake_bucket

    analysis_id = 'A2'
    aliquot_id = '2'

    # Load completed analysis for aliquot_id=2
    clin_minio.load_string("{}",
                           build_s3_analyses_json_key(analysis_id, aliquot_id, franklin_analysis_id='123'),
                           bucket_name=clin_datalake_bucket, replace=True)

    assert can_create_analysis(clin_minio, trio_analysis_rows) is False


def test_can_create_analysis_with_in_progress_analyses(clin_minio, trio_analysis_rows):
    """
    It should return False if there are existing Franklin analyses in progress for the given analyses.
    """
    from lib.franklin import can_create_analysis, build_s3_analyses_status_key
    from lib.config import clin_datalake_bucket

    analysis_id = 'A2'
    aliquot_id = '2'

    # Load in-progress analysis for aliquot_id=2
    clin_minio.load_string("CREATED",
                           build_s3_analyses_status_key(analysis_id, aliquot_id),
                           bucket_name=clin_datalake_bucket, replace=True)

    assert can_create_analysis(clin_minio, trio_analysis_rows) is False


def test_transfer_vcf_to_franklin(clin_minio, franklin_s3, load_vcfs, with_vcf, solo_analysis_row, trio_analysis_rows):
    """
    It should transfer VCF files from the Clin S3 bucket to the Franklin S3 bucket.
    """
    from lib.config import clin_import_bucket, env, s3_franklin_bucket, clin_nextflow_bucket
    from lib.franklin import transfer_vcf_to_franklin, build_s3_franklin_vcf_key

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
    for analysis in analyses:
        analysis_id = analysis['analysis_id']
        vcf_key = analysis['vcf_key']
        destination_key = build_s3_franklin_vcf_key(env, analysis_id, vcf_key)
        franklin_s3_response = franklin_s3.read_key(destination_key, bucket_name=s3_franklin_bucket)
        assert franklin_s3_response == f'VCF content for {vcf_key}', f'Content mismatch for {vcf_key}'


def test_build_create_analysis_payload_for_family_with_hpo(clin_minio, franklin_s3, trio_analysis_rows, with_vcf,
                                                           load_vcfs):
    """
    It should build the payload for creating analyses in Franklin.
    """
    from lib.config import s3_franklin_bucket, franklin_assay_id, env
    from lib.franklin import build_create_analysis_payload, build_s3_franklin_vcf_key

    family_id = 'FM1'
    analysis_id = 'A2'
    vcf_key = 'post_processing/output/ensemblvep/variants.A2.snv.norm.VEP.vcf.gz'
    franklin_vcf_key = build_s3_franklin_vcf_key(env, analysis_id, vcf_key)
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
                    'sample_name': f'2 - {analysis_id}',
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
                    'sample_name': f'3 - {analysis_id}',
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
                    'sample_name': f'4 - {analysis_id}',
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
                'case_name': f'family - {analysis_id}',
                'family_samples': [
                    {
                        'sample_name': f'2 - {analysis_id}',
                        'family_relation': 'proband',
                        'is_affected': True,
                    },
                    {
                        'sample_name': f'3 - {analysis_id}',
                        'family_relation': 'father',
                        'is_affected': False,
                    },
                    {
                        'sample_name': f'4 - {analysis_id}',
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
    from lib.franklin import build_create_analysis_payload, build_s3_franklin_vcf_key

    analysis_id = 'A1'
    vcf_key = 'BATCH_1/1.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    franklin_vcf_key = build_s3_franklin_vcf_key(env, analysis_id, vcf_key)
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
                    'sample_name': f'1 - {analysis_id}',
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
    from lib.franklin import get_franklin_token, post_create_analysis, build_s3_franklin_vcf_key

    analysis_id = 'A2'
    family_id = 'FM1'
    vcf_key = 'post_processing/output/ensemblvep/variants.A2.snv.norm.VEP.vcf.gz'
    franklin_vcf_key = build_s3_franklin_vcf_key(env, analysis_id, vcf_key)
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
    from lib.franklin import write_s3_analyses_status, FranklinStatus, build_s3_analyses_status_key, \
        build_s3_analyses_ids_key
    from lib.config import clin_datalake_bucket

    analysis_id = 'A2'
    status = FranklinStatus.CREATED
    franklin_ids = [11, 22, 33]

    write_s3_analyses_status(clin_minio, trio_analysis_rows, status, franklin_ids)

    # Check if all status files were created
    for analysis in trio_analysis_rows:
        aliquot_id = analysis['aliquot_id']
        status_key = build_s3_analyses_status_key(analysis_id, aliquot_id)
        assert clin_minio.read_key(status_key, bucket_name=clin_datalake_bucket) == status.name

    # Check if the ids file was created
    ids_file_key = build_s3_analyses_ids_key(analysis_id)
    assert clin_minio.read_key(ids_file_key, bucket_name=clin_datalake_bucket) == '11,22,33'


def test_write_s3_analysis_status_for_solo_with_id(clin_minio, solo_analysis_row):
    """
    It should write the analysis status and id to the S3 bucket for a solo analysis.
    """
    from lib.franklin import write_s3_analysis_status, FranklinStatus, build_s3_analyses_status_key, \
        build_s3_analyses_id_key
    from lib.config import clin_datalake_bucket

    analysis_id = solo_analysis_row['analysis_id']
    aliquot_id = solo_analysis_row['aliquot_id']
    status = FranklinStatus.READY
    franklin_id = '11'

    write_s3_analysis_status(clin_minio, analysis_id, aliquot_id, status, id=franklin_id)

    # Check if the status file was created
    status_key = build_s3_analyses_status_key(analysis_id, aliquot_id)
    assert clin_minio.read_key(status_key, bucket_name=clin_datalake_bucket) == status.name

    # Check if the id file was created
    ids_file_key = build_s3_analyses_id_key(analysis_id, aliquot_id)
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
def test_get_analysis_status(clin_minio, franklin_s3, with_vcf, load_vcfs, trio_analysis_rows):
    """
    It should return the status of the analysis from Franklin.
    """
    from lib.config import env, s3_franklin_bucket
    from lib.franklin import get_franklin_token, get_analysis_status, post_create_analysis, build_s3_franklin_vcf_key

    family_id = 'FM1'
    analysis_id = 'A2'
    vcf_key = 'post_processing/output/ensemblvep/variants.A2.snv.norm.VEP.vcf.gz'
    franklin_vcf_key = build_s3_franklin_vcf_key(env, analysis_id, vcf_key)
    analyses = with_vcf(trio_analysis_rows, s3_franklin_bucket, vcf_key)

    # Load VCF into the Franklin S3 bucket
    load_vcfs(franklin_s3, s3_franklin_bucket, [franklin_vcf_key])

    token = get_franklin_token()
    franklin_ids = post_create_analysis(family_id, analyses, token, franklin_s3)
    statuses = get_analysis_status(franklin_ids, token)

    assert len(statuses) == 4  # 3 analyses + 1 family analysis
    for status in statuses:
        assert status['status'] == 'ACTIVE'
        assert status['processing_status'] in ['PROCESSING_ANNOTATION', 'READY']


@pytest.mark.vpn
def test_get_completed_analysis_not_ready(clin_minio, franklin_s3, with_vcf, load_vcfs, solo_analysis_row):
    """
    It should fail if the analysis is not ready.
    """
    from lib.config import env, s3_franklin_bucket
    from lib.franklin import get_franklin_token, get_completed_analysis, post_create_analysis, build_s3_franklin_vcf_key

    analysis_id = 'A1'
    vcf_key = 'BATCH_1/1.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    franklin_vcf_key = build_s3_franklin_vcf_key(env, analysis_id, vcf_key)
    analyses = with_vcf([solo_analysis_row], s3_franklin_bucket, vcf_key)

    # Load VCF into the Franklin S3 bucket
    load_vcfs(franklin_s3, s3_franklin_bucket, [franklin_vcf_key])

    token = get_franklin_token()
    franklin_id = post_create_analysis(None, analyses, token, franklin_s3)[0]

    with pytest.raises(AirflowFailException) as e:
        get_completed_analysis(franklin_id, token)
        assert str(e.value) == f'Error from Franklin API call: analysis not ready: {franklin_id}'


@pytest.mark.vpn
@pytest.mark.slow
def test_get_completed_analysis_ready(clin_minio, franklin_s3, with_vcf, load_vcfs, solo_analysis_row):
    """
    It should return the response if the analysis is ready.
    """
    from lib.config import env, s3_franklin_bucket
    from lib.franklin import get_franklin_token, get_analysis_status, get_completed_analysis, post_create_analysis, \
        build_s3_franklin_vcf_key

    analysis_id = 'A1'
    vcf_key = 'BATCH_1/1.case.hard-filtered.formatted.norm.VEP.vcf.gz'
    franklin_vcf_key = build_s3_franklin_vcf_key(env, analysis_id, vcf_key)
    analyses = with_vcf([solo_analysis_row], s3_franklin_bucket, vcf_key)

    # Load VCF into the Franklin S3 bucket
    load_vcfs(franklin_s3, s3_franklin_bucket, [franklin_vcf_key])

    token = get_franklin_token()
    franklin_id = post_create_analysis(None, analyses, token, franklin_s3)[0]

    # Poll for READY status
    timeout_seconds = 80
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
