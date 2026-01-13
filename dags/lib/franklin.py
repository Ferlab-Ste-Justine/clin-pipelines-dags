import http.client
import json
import logging
import urllib.parse
from collections import defaultdict
from enum import Enum
from typing import Dict, List, Optional, Tuple

from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib import config
from lib.config import (clin_datalake_bucket, clin_import_bucket,
                        clin_nextflow_bucket, env, franklin_assay_id)
from lib.config_nextflow import nextflow_post_processing_vep_output_key
from lib.datasets import enriched_clinical
from lib.utils_etl import ClinVCFSuffix
from lib.utils_etl_tables import to_pandas
from pandas import DataFrame


# current state of an analysis is saved inside _FRANKLIN_STATUS_.txt
class FranklinStatus(Enum):
    UNKNOWN = 0  # equivalent to never created / not found
    CREATED = 1  # analysis created
    READY = 3  # status of the analysis is READY
    COMPLETED = 4  # we have successfully retrieved the JSON and save into S3


class FamilyMember(Enum):
    PROBAND = 'PROBAND'
    MOTHER = 'MTH'
    FATHER = 'FTH'


franklin_url_parts = urllib.parse.urlparse(config.franklin_url)
family_analysis_keyword = 'family'


def get_clinical_data(analysis_ids: List[str]) -> List[dict]:
    import numpy as np

    clinical_df: DataFrame = to_pandas(enriched_clinical.uri)
    filtered_df = clinical_df[clinical_df['analysis_id'].isin(analysis_ids)]
    selected_columns = ['analysis_id', 'family_id', 'aliquot_id', 'sequencing_id', 'batch_id', 'is_proband',
                        'father_aliquot_id', 'mother_aliquot_id', 'affected_status', 'first_name', 'birth_date',
                        'gender', 'clinical_signs']

    filtered_df = filtered_df[selected_columns].copy()

    # Convert 'clinical_signs' to a list of dicts with only 'id', using native Python types
    def extract_ids(clinical_signs):
        if isinstance(clinical_signs, (list, np.ndarray)):
            return [{'id': str(cs['id'])} for cs in clinical_signs if 'id' in cs]
        return None

    filtered_df['clinical_signs'] = filtered_df['clinical_signs'].apply(extract_ids)

    return filtered_df.to_dict(orient='records')


def group_families_from_clinical_data(clinical_data: List[dict]) -> Tuple[Dict[str, List[dict]], List[dict]]:
    # Separate rows with and without family_id
    family_analyses = [row for row in clinical_data if row.get('family_id') is not None]
    solo_analyses = [row for row in clinical_data if row.get('family_id') is None]

    # Group rows by family_id
    family_groups: Dict[str, List[dict]] = defaultdict(list)
    for row in family_analyses:
        family_id = row['family_id']
        family_groups[family_id].append(row)

    return family_groups, solo_analyses


def filter_valid_families(family_groups: Dict[str, List[dict]]) -> Dict[str, List[dict]]:
    filtered_families = {}
    for family_id, analyses in family_groups.items():
        has_proband = False
        has_mother = False
        has_father = False
        has_something_else = None

        for analysis in analyses:
            # Proband row contains the parent information
            if analysis['is_proband']:
                has_proband = True

                if analysis['father_aliquot_id']:
                    has_father = True

                if analysis['mother_aliquot_id']:
                    has_mother = True

            # If we have a brother or a sister
            if not analysis['is_proband'] and (analysis['father_aliquot_id'] or analysis['mother_aliquot_id']):
                has_something_else = True
                logging.warning(f'Unknown relation in family {family_id} for sequencing id {analysis["sequencing_id"]}')

        if not has_something_else and has_proband and (has_mother or has_father):  # TRIO or DUO
            filtered_families[family_id] = analyses
        else:
            logging.warning(
                f'(unsupported) family: {family_id} with PROBAND: {has_proband} MOTHER: {has_mother} FATHER: {has_father} UNSUPPORTED: {has_something_else} analyses: {analyses}')
    return filtered_families


def transfer_vcf_to_franklin(s3_clin, s3_franklin, analyses):
    for analysis in analyses:
        analysis_id = analysis['analysis_id']
        source_bucket = analysis['vcf_bucket']
        source_key = analysis['vcf_key']
        destination_key = build_s3_franklin_vcf_key(env, analysis_id, source_key)
        if not s3_franklin.check_for_key(destination_key, config.s3_franklin_bucket):
            logging.info(f'Retrieve VCF content: {source_bucket}/{source_key}')
            vcf_file = s3_clin.get_key(source_key, source_bucket)
            vcf_content = vcf_file.get()['Body'].read()
            logging.info(f'VCF content size: {len(vcf_content)}')
            logging.info(f'Upload to Franklin: {config.s3_franklin_bucket}/{destination_key}')
            s3_franklin.load_bytes(vcf_content, destination_key, config.s3_franklin_bucket, replace=True)
        else:
            logging.info(f'Already on Franklin: {config.s3_franklin_bucket}/{destination_key}')


def extract_vcf_prefix(vcf_key):
    name = vcf_key.split('/')[-1] \
        .replace(ClinVCFSuffix.SNV_GERMLINE_LEGACY.value, '') \
        .replace(ClinVCFSuffix.SNV_GERMLINE.value, '') \
        .replace('.case', '') \
        .replace('.snv', '') \
        .replace('variants.', '')
    if name is None or len(name) == 0:  # robustness
        raise AirflowFailException(f'Invalid VCF prefix: {vcf_key}')
    return name


def _get_nextflow_vcf_paths(clin_s3: S3Hook) -> dict[str, dict]:
    vcfs = {}
    nextflow_vcf_keys = clin_s3.list_keys(clin_nextflow_bucket, nextflow_post_processing_vep_output_key)
    for key in nextflow_vcf_keys:
        if key.endswith(ClinVCFSuffix.SNV_GERMLINE.value):
            vcfs[key] = {
                'bucket': clin_nextflow_bucket,
                'prefix': extract_vcf_prefix(key)
            }

    return vcfs


def _get_legacy_vcf_paths(clin_s3: S3Hook, batch_id: str) -> dict[str, dict]:
    vcfs = {}
    legacy_vcf_keys = clin_s3.list_keys(clin_import_bucket, f'{batch_id}/')
    for key in legacy_vcf_keys:
        if key.endswith(ClinVCFSuffix.SNV_GERMLINE_LEGACY.value):
            vcfs[key] = {
                'bucket': clin_import_bucket,
                'prefix': extract_vcf_prefix(key)
            }
    return vcfs


def attach_vcf_to_analysis(clin_s3: S3Hook, analysis: dict, vcfs: dict[str, dict],
                           proband_aliquot_id: Optional[str]) -> dict:
    aliquot_id = analysis['aliquot_id']
    family_id = analysis['family_id']  # solo will be None
    analysis_id = analysis['analysis_id']
    batch_id = analysis['batch_id']

    vcf_bucket = None
    vcf_key = None
    # Try to get the VCF from Nextflow output using the analysis id
    for key, info in vcfs.items():
        prefix = info['prefix']
        bucket = info['bucket']
        if analysis_id == prefix:
            vcf_bucket = bucket
            vcf_key = key
            break

    # Fallback to the legacy method (check in the batch_id folder)
    if not vcf_key:
        legacy_vcfs = _get_legacy_vcf_paths(clin_s3, batch_id)
        for key, info in legacy_vcfs.items():
            prefix = info['prefix']
            bucket = info['bucket']
            # if we have only one VCF and the prefix match aliquot_id or family_id
            if (len(legacy_vcfs) == 1) or (aliquot_id == prefix) or (family_id == prefix):
                vcf_bucket = bucket
                vcf_key = key
                break
        # last resort => use the proband VCF (should contain the entire family)
        for key, info in legacy_vcfs.items():
            prefix = info['prefix']
            bucket = info['bucket']
            if proband_aliquot_id == prefix:
                vcf_bucket = bucket
                vcf_key = key
                break

        # did we miss one during extraction ?
        if not vcf_key:
            raise AirflowFailException(f'No VCF to attach for aliquot_id {aliquot_id}')

    # attach the VCF to the analysis
    analysis['vcf_bucket'] = vcf_bucket
    analysis['vcf_key'] = vcf_key
    logging.info(
        f'Attach VCF for aliquot {aliquot_id} in family {family_id}. VCF bucket: {vcf_bucket}. VCF key: {vcf_key}.')
    return analysis


def find_proband_aliquot_id(analyses):
    for analysis in analyses:
        if analysis["is_proband"]:
            return analysis["aliquot_id"]
    raise AirflowFailException(f'Cant find proband aliquot id in: {analyses}')


# add a 'vcf' field to the analyses
def attach_vcf_to_analyses(clin_s3: S3Hook, family_groups: dict):
    families = family_groups['families']
    solos = family_groups['no_family']

    vcfs = _get_nextflow_vcf_paths(clin_s3)

    for family_id, analyses in families.items():
        proband_aliquot_id = find_proband_aliquot_id(analyses)
        for analysis in analyses:
            attach_vcf_to_analysis(clin_s3, analysis, vcfs, proband_aliquot_id)
    for patient in solos:
        attach_vcf_to_analysis(clin_s3, patient, vcfs, None)

    return family_groups


def get_s3_key_content_size(s3, bucket, key):
    if (s3.check_for_key(key, bucket)):
        file = s3.get_key(key, bucket)
        file_content = file.get()['Body'].read()
        return len(file_content)
    return 0


# avoid spamming franklin <!>
def can_create_analysis(clin_s3, analyses):
    for analysis in analyses:
        analysis_id = analysis["analysis_id"]
        aliquot_id = analysis["aliquot_id"]
        completed_analysis_keys = clin_s3.list_keys(clin_datalake_bucket,
                                                    build_s3_analyses_root_key_with_aliquot_id(analysis_id, aliquot_id))
        for key in completed_analysis_keys:
            if 'franklin_analysis_id=' in key:  # found at least one completed analysis
                logging.info(f'Completed analysis found: analysis_id {analysis_id} aliquot_id {aliquot_id}')
                return False
        status = check_s3_analysis_status(clin_s3, analysis_id, aliquot_id)
        if status != FranklinStatus.UNKNOWN:  # fund at least one analysis with a STATUS
            logging.info(f'Created analysis found: analysis_id {analysis_id} aliquot_id {aliquot_id}')
            return False
    return True


def check_s3_analysis_status(clin_s3, analysis_id, aliquot_id) -> FranklinStatus:
    key = build_s3_analyses_status_key(analysis_id, aliquot_id)
    if (clin_s3.check_for_key(key, clin_datalake_bucket)):
        file = clin_s3.get_key(key, clin_datalake_bucket)
        file_content = file.get()['Body'].read()
        return FranklinStatus[file_content.decode('utf-8')]
    return FranklinStatus.UNKNOWN  # analysis doesn't exist


def write_s3_analyses_status(clin_s3, analyses, status, ids=None):
    for analysis in analyses:
        analysis_id = analysis['analysis_id']
        aliquot_id = analysis['aliquot_id']
        write_s3_analysis_status(clin_s3, analysis_id, aliquot_id, status, ids)


def write_s3_analysis_status(clin_s3, analysis_id, aliquot_id, status, ids=None, id=None):
    clin_s3.load_string(status.name, build_s3_analyses_status_key(analysis_id, aliquot_id),
                        clin_datalake_bucket, replace=True)
    if ids is not None:  # save TRIO, DUO ... analyses IDs
        clin_s3.load_string(','.join(map(str, ids)), build_s3_analyses_ids_key(analysis_id),
                            clin_datalake_bucket, replace=True)
    if id is not None:  # after status, we can attach an ID to a specific family + aliquot id whether it's SOLO or TRIO, DUO ...
        clin_s3.load_string(str(id), build_s3_analyses_id_key(analysis_id, aliquot_id), clin_datalake_bucket,
                            replace=True)


def build_s3_analyses_root_key(analysis_id):
    return f'raw/landing/franklin/analysis_id={analysis_id}'


def build_s3_analyses_root_key_with_aliquot_id(analysis_id, aliquot_id):
    return f'{build_s3_analyses_root_key(analysis_id)}/aliquot_id={aliquot_id or "null"}'


def build_s3_analyses_json_key(analysis_id, aliquot_id, franklin_analysis_id):
    return f'{build_s3_analyses_root_key_with_aliquot_id(analysis_id, aliquot_id)}/franklin_analysis_id={franklin_analysis_id}/analysis.json'


def build_s3_analyses_status_key(analysis_id, aliquot_id):
    return f'{build_s3_analyses_root_key_with_aliquot_id(analysis_id, aliquot_id)}/_FRANKLIN_STATUS_.txt'


def build_s3_analyses_id_key(analysis_id, aliquot_id):
    return f'{build_s3_analyses_root_key_with_aliquot_id(analysis_id, aliquot_id)}/_FRANKLIN_ID_.txt'


def build_s3_analyses_ids_key(analysis_id):
    return f'{build_s3_analyses_root_key(analysis_id)}/_FRANKLIN_IDS_.txt'


def build_s3_franklin_root_key(env: str, analysis_id: str) -> str:
    return f'{env}/{analysis_id}'


def build_s3_franklin_vcf_key(env: str, analysis_id: str, vcf_key: str):
    return f'{build_s3_franklin_root_key(env, analysis_id)}/{vcf_key}'


def get_s3_analyses_keys(clin_s3: S3Hook, analysis_ids: List[str]) -> List[str]:
    keys = []
    for analysis_id in analysis_ids:
        keys += clin_s3.list_keys(clin_datalake_bucket, build_s3_analyses_root_key(analysis_id))
    return keys


def delete_franklin_s3_data(clin_s3: S3Hook, analysis_ids: List[str]) -> None:
    logging.info(f'About to delete Franklin data for {len(analysis_ids)} analysis IDs: {analysis_ids}')
    
    total_deleted = 0
    
    for analysis_id in analysis_ids:
        prefix = build_s3_analyses_root_key(analysis_id)
        keys = clin_s3.list_keys(clin_datalake_bucket, prefix)
        
        if keys:
            clin_s3.delete_objects(clin_datalake_bucket, keys)
            deleted_count = len(keys)
            total_deleted += deleted_count
            logging.info(f'Deleted {deleted_count} objects for analysis_id {analysis_id} at {prefix}')
        else:
            logging.warning(f'No S3 objects found for analysis_id {analysis_id} at {prefix}')
    
    logging.info(f'Successfully deleted {total_deleted} objects across {len(analysis_ids)} analysis IDs')


# extract_param_from_s3_key('raw/landing/franklin/analysis_id=foo' ,'analysis_id') -> foo
def extract_param_from_s3_key(key, param_name):
    for param in key.split('/'):
        if param.startswith(f"{param_name}="):
            value = param.split('=')[1]
            if value == 'null':
                return None  # we want to have None when dealing with null in S3
            else:
                return value
    raise AirflowFailException(f'Cant find param: {param_name} in s3 key: {key}')


name_separator = " - "


def build_sample_name(aliquot_id, analysis_id):
    return f'{aliquot_id}{name_separator}{analysis_id}'  # seems to convert SOLO family_id to 'None' as a str


def extract_from_name_aliquot_id(name):
    _id = name.split(name_separator)[0].strip()
    # family analysis has no aliquot
    return _id if _id != family_analysis_keyword else None


def extract_from_name_analysis_id(name):
    _id = name.split(name_separator)[1].strip()  # cf build_sample_name(),
    if '_' in _id:
        _id = _id.split('_')[0] # remove possible suffix added by franklin due to re-analysing or reset
    if _id == 'None':
        _id = None
    return _id 


def get_family_relation(aliquot_id: str, proband_aliquot_id: str, father_aliquot_id: str,
                        mother_aliquot_id: str) -> str:
    if aliquot_id == father_aliquot_id:
        return 'father'
    elif aliquot_id == mother_aliquot_id:
        return 'mother'
    elif aliquot_id == proband_aliquot_id:
        return 'proband'
    else:
        raise AirflowFailException(f'Missing relation for aliquot {aliquot_id}')


def build_create_analysis_payload(family_id: Optional[str], analyses: List[dict], franklin_s3: S3Hook):
    family_analyses = []
    analyses_payload = []

    # Find proband row to get family relation info
    proband_row = next(a for a in analyses if a['is_proband'])
    analysis_id = proband_row['analysis_id']
    proband_aliquot_id = proband_row['aliquot_id']
    father_aliquot_id = proband_row['father_aliquot_id']
    mother_aliquot_id = proband_row['mother_aliquot_id']
    clinical_signs = proband_row['clinical_signs'] or []  # Replace None with empty list
    phenotypes = [cs['id'] for cs in clinical_signs if 'id' in cs]

    for analysis in analyses:
        aliquot_id = analysis['aliquot_id']
        vcf_key = analysis['vcf_key']
        affected_status = analysis['affected_status']
        first_name = analysis['first_name']
        birth_date = str(analysis['birth_date'])  # date type is not JSON serializable, convert to str
        gender = analysis['gender']

        sample_name = build_sample_name(aliquot_id, analysis_id)
        sample = {
            "sample_name": sample_name,
            "family_relation": get_family_relation(aliquot_id, proband_aliquot_id, father_aliquot_id,
                                                   mother_aliquot_id),
            "is_affected": affected_status
        }
        if family_id:
            family_analyses.append(sample)

        vcf_franklin_s3_full_path = build_s3_franklin_vcf_key(env, analysis_id, vcf_key)
        # last resort before going further
        # check if the VCF exists in Franklin S3
        if franklin_s3.check_for_key(vcf_franklin_s3_full_path, config.s3_franklin_bucket):
            analyses_payload.append({
                "assay_id": franklin_assay_id,
                'sample_data': {
                    "sample_name": sample_name,
                    "name_in_vcf": aliquot_id,
                    "aws_files": [
                        {
                            "key": vcf_franklin_s3_full_path,
                            "type": "VCF_SHORT"
                        }
                    ],
                    "tissue_type": "Whole Blood",
                    "patient_details": {
                        "name": first_name,
                        "dob": birth_date,
                        "sex": gender
                    }
                }
            })
        else:
            raise AirflowFailException(f'VCF not found: {config.s3_franklin_bucket}/{vcf_franklin_s3_full_path}')

    payload = {
        'upload_specs': {
            "source": 'AWS',
            "details": {
                "bucket": config.s3_franklin_bucket,
                'root_folder': '/'
            }
        },
        'analyses': analyses_payload,
    }

    if family_id:
        payload['family_analyses'] = [
            {
                'case_name': build_sample_name(family_analysis_keyword, analysis_id),
                'family_samples': family_analyses,
                "phenotypes": phenotypes
            }
        ]
        payload["family_analyses_creation_specs"] = {
            "create_family_single_analyses": 'true'
        }

    return payload


def parse_response(res, log_body=True):
    data = res.read()
    body = data.decode('utf-8')
    if log_body is True:
        logging.info(f'{res.status} - {body}')
    else:
        logging.info(f'{res.status}')
    if res.status != 200:  # log if something wrong
        raise AirflowFailException(f'Error from Franklin API call: {body}')
    return body


def parse_response_json(res):
    return json.loads(parse_response(res))


def get_franklin_http_conn():
    if config.franklin_url.startswith('https'):
        conn = http.client.HTTPSConnection(franklin_url_parts.hostname)
    else:
        conn = http.client.HTTPConnection(franklin_url_parts.hostname, port=franklin_url_parts.port)
    return conn


def get_franklin_token(current_token=None):
    if current_token is None:
        conn = get_franklin_http_conn()
        payload = urllib.parse.urlencode({'email': config.franklin_email, 'password': config.franklin_password})
        conn.request("GET", franklin_url_parts.path + '/v1/auth/login?' + payload)
        conn.close
        return parse_response_json(conn.getresponse())['token']
    return current_token


def post_create_analysis(family_id: Optional[str], analyses: List[dict], token: str, franklin_s3: S3Hook):
    conn = get_franklin_http_conn()
    headers = {'Content-Type': "application/json", 'Authorization': "Bearer " + token}
    payload = json.dumps(build_create_analysis_payload(family_id, analyses, franklin_s3)).encode('utf-8')
    logging.info(f'Create analysis: {payload}')
    conn.request("POST", franklin_url_parts.path + "/v1/analyses/create", payload, headers)
    conn.close
    return parse_response_json(conn.getresponse())


def get_analysis_status(started_analyses, token, chunk_size=10):
    conn = get_franklin_http_conn()
    headers = {'Content-Type': "application/json", 'Authorization': "Bearer " + token}

    # Split started_analyses into chunks
    analysis_chunks = [started_analyses[i:i + chunk_size] for i in range(0, len(started_analyses), chunk_size)]
    response_data = []

    for chunk in analysis_chunks:
        payload = json.dumps({'analysis_ids': chunk}).encode('utf-8')
        logging.info(f'Get analysis status: {payload}')
        conn.request("POST", franklin_url_parts.path + "/v1/analyses/status", payload, headers)
        response = conn.getresponse()
        response_data.extend(parse_response_json(response))

    conn.close()
    return response_data


def get_completed_analysis(id, token):
    conn = get_franklin_http_conn()
    headers = {'Content-Type': "application/json", 'Authorization': "Bearer " + token}
    logging.info(f'Get completed analysis: {id}')
    conn.request("GET", franklin_url_parts.path + f"/v2/analysis/variants/snp?analysis_id={id}", "", headers)
    conn.close
    return parse_response(conn.getresponse(), False)
