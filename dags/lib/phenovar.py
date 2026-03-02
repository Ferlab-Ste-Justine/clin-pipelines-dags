import http.client
import json
import logging
import urllib.parse
from enum import Enum
from typing import Dict, List, Optional

from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib import config
from lib.config import clin_datalake_bucket, clin_phenovar_import_bucket
from lib.datasets import enriched_clinical
from lib.utils_etl_tables import to_pandas
from pandas import DataFrame


# Current state of a Phenovar analysis is saved inside _PHENOVAR_STATUS_.txt
class PhenotypingStatus(Enum):
    UNKNOWN = 0  # equivalent to never created / not found
    PENDING = 1  # task submitted to Phenovar API
    STARTED = 2  # Phenovar is processing
    SUCCESS = 3  # Successfully completed
    FAILURE = 4  # Failed processing


phenovar_url_parts = urllib.parse.urlparse(config.phenovar_url)


def get_clinical_data(analysis_ids: List[str]) -> List[dict]:
    """Extract clinical data from enriched_clinical table for given analysis IDs."""
    import numpy as np

    clinical_df: DataFrame = to_pandas(enriched_clinical.uri)
    filtered_df = clinical_df[clinical_df['analysis_id'].isin(analysis_ids)]
    selected_columns = [
        'analysis_id', 'sequencing_id', 'family_id', 'aliquot_id', 'is_proband',
        'father_aliquot_id', 'mother_aliquot_id', 'affected_status',
        'gender', 'clinical_signs', 'patient_id',
        'snv_vcf_germline_urls', 'cnv_vcf_germline_urls'
    ]

    filtered_df = filtered_df[selected_columns].copy()

    # Convert 'clinical_signs' to a list of HPO codes
    def extract_hpo_codes(clinical_signs):
        if isinstance(clinical_signs, (list, np.ndarray)):
            return [str(cs['id']) for cs in clinical_signs if 'id' in cs]
        return []

    filtered_df['clinical_signs'] = filtered_df['clinical_signs'].apply(extract_hpo_codes)

    return filtered_df.to_dict(orient='records')


def parse_s3_url(url_str: str) -> tuple[str, str]:
    """
    Parse S3 URL from WrappedArray format.
    Example: WrappedArray(s3a://cqgc-qa-app-download/blue/254bad63.vcf.gz) 
    Returns: ('cqgc-qa-app-download', 'blue/254bad63.vcf.gz')
    """
    # Remove WrappedArray() wrapper if present
    url_str = url_str.strip()
    if url_str.startswith('WrappedArray('):
        url_str = url_str[13:-1]  # Remove 'WrappedArray(' and ')'
    
    # Parse s3a:// or s3:// URL
    if url_str.startswith('s3a://') or url_str.startswith('s3://'):
        parts = url_str.split('://', 1)[1].split('/', 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ''
        return bucket, key
    
    raise AirflowFailException(f'Invalid S3 URL format: {url_str}')


def extract_vcf_filename(s3_key: str) -> str:
    """Extract filename from S3 key (e.g., 'blue/254bad63.vcf.gz' -> '254bad63.vcf.gz')."""
    return s3_key.split('/')[-1]


def map_phenovar_file_type(is_proband: bool, is_mother: bool, is_father: bool, is_snv: bool) -> str:
    """
    Map to Phenovar file type based on family relation and variant type.
    Returns one of: patient_called_snv, patient_called_cnv, maternal_called_snv, 
    maternal_called_cnv, paternal_called_snv, paternal_called_cnv
    """
    relation = 'patient' if is_proband else ('maternal' if is_mother else 'paternal')
    variant_type = 'snv' if is_snv else 'cnv'
    return f'{relation}_called_{variant_type}'


def copy_vcf_to_phenovar_bucket(clin_s3: S3Hook, phenovar_s3: S3Hook, analysis_id: str, 
                                 source_bucket: str, source_key: str, dest_filename: str) -> None:
    """Copy VCF file from source bucket to Phenovar import bucket."""
    destination_key = f'{analysis_id}/{dest_filename}'
    
    if phenovar_s3.check_for_key(destination_key, clin_phenovar_import_bucket):
        logging.info(f'VCF already in Phenovar bucket: {clin_phenovar_import_bucket}/{destination_key}')
        return
    
    logging.info(f'Copying VCF: {source_bucket}/{source_key} -> {clin_phenovar_import_bucket}/{destination_key}')
    vcf_file = clin_s3.get_key(source_key, source_bucket)
    vcf_content = vcf_file.get()['Body'].read()
    logging.info(f'VCF content size: {len(vcf_content)} bytes')
    phenovar_s3.load_bytes(vcf_content, destination_key, clin_phenovar_import_bucket, replace=True)


def build_phenovar_payload(analysis_data: dict, vcf_files: List[dict]) -> dict:
    """
    Build Phenovar API payload from clinical data and VCF file list.
    
    analysis_data: dict with keys: sequencing_id, analysis_id, gender, clinical_signs, 
                   maternal_affected, paternal_affected
    vcf_files: list of dicts with keys: filepath, filetype
    """
    payload = {
        "schema_version": "1.0.0",
        "patient_details": {
            "externalid": analysis_data['sequencing_id'],
            "sex": analysis_data['gender'],
            "maternal_affected": analysis_data.get('maternal_affected', False),
            "paternal_affected": analysis_data.get('paternal_affected', False),
            "label": "",
            "cohort": analysis_data['analysis_id']
        },
        "phenotype_hpo_code_list": analysis_data.get('clinical_signs', []),
        "phenotype_onset_hpo_code": "",  # TODO: Extract earliest onset if available
        "download_specifications": {
            "download_source": "cqgc_s3",  # This should match Phenovar server config
            "details": {
                "s3_bucket_name": clin_phenovar_import_bucket,
                "s3_bucket_root_path": analysis_data['analysis_id']
            }
        },
        "analysis_files": vcf_files
    }
    
    return payload


def build_s3_phenovar_root_key(analysis_id: str) -> str:
    """Build S3 key prefix for Phenovar data."""
    return f'raw/landing/phenovar/analysis_id={analysis_id}'


def build_s3_status_key(analysis_id: str) -> str:
    """Build S3 key for status marker file."""
    return f'{build_s3_phenovar_root_key(analysis_id)}/_PHENOVAR_STATUS_.txt'


def build_s3_task_id_key(analysis_id: str) -> str:
    """Build S3 key for task ID marker file."""
    return f'{build_s3_phenovar_root_key(analysis_id)}/_PHENOVAR_TASK_ID_.txt'


def build_s3_result_key(analysis_id: str) -> str:
    """Build S3 key for result JSON file."""
    return f'{build_s3_phenovar_root_key(analysis_id)}/phenovar_result.json'


def check_s3_analysis_status(clin_s3: S3Hook, analysis_id: str) -> PhenotypingStatus:
    """Check the current status of an analysis from S3 marker file."""
    key = build_s3_status_key(analysis_id)
    if clin_s3.check_for_key(key, clin_datalake_bucket):
        file = clin_s3.get_key(key, clin_datalake_bucket)
        file_content = file.get()['Body'].read()
        return PhenotypingStatus[file_content.decode('utf-8')]
    return PhenotypingStatus.UNKNOWN


def read_s3_task_id(clin_s3: S3Hook, analysis_id: str) -> Optional[str]:
    """Read Phenovar task ID from S3 marker file."""
    key = build_s3_task_id_key(analysis_id)
    if clin_s3.check_for_key(key, clin_datalake_bucket):
        file = clin_s3.get_key(key, clin_datalake_bucket)
        return file.get()['Body'].read().decode('utf-8')
    return None


def write_s3_analysis_status(clin_s3: S3Hook, analysis_id: str, status: PhenotypingStatus, 
                             task_id: Optional[str] = None) -> None:
    """Write analysis status (and optionally task ID) to S3 marker files."""
    clin_s3.load_string(
        status.name,
        build_s3_status_key(analysis_id),
        clin_datalake_bucket,
        replace=True
    )
    
    if task_id is not None:
        clin_s3.load_string(
            task_id,
            build_s3_task_id_key(analysis_id),
            clin_datalake_bucket,
            replace=True
        )


def can_submit_analysis(clin_s3: S3Hook, analysis_id: str) -> bool:
    """
    Check if we can submit a new analysis request to Phenovar.
    Returns False if analysis already exists and is not failed.
    """
    status = check_s3_analysis_status(clin_s3, analysis_id)
    
    if status == PhenotypingStatus.UNKNOWN:
        return True  # No existing analysis
    
    if status == PhenotypingStatus.FAILURE:
        logging.info(f'Previous analysis failed for {analysis_id}, allowing resubmission')
        return True
    
    if status == PhenotypingStatus.SUCCESS:
        # Check if result file exists
        result_key = build_s3_result_key(analysis_id)
        if clin_s3.check_for_key(result_key, clin_datalake_bucket):
            logging.info(f'Completed analysis found for {analysis_id}')
            return False
    
    logging.info(f'Analysis in progress for {analysis_id} with status {status.name}')
    return False


def delete_phenovar_s3_data(clin_s3: S3Hook, analysis_ids: List[str]) -> None:
    """Delete all Phenovar S3 data for given analysis IDs (for reset functionality)."""
    logging.info(f'About to delete Phenovar data for {len(analysis_ids)} analysis IDs: {analysis_ids}')
    
    total_deleted = 0
    
    for analysis_id in analysis_ids:
        prefix = build_s3_phenovar_root_key(analysis_id)
        keys = clin_s3.list_keys(clin_datalake_bucket, prefix)
        
        if keys:
            clin_s3.delete_objects(clin_datalake_bucket, keys)
            deleted_count = len(keys)
            total_deleted += deleted_count
            logging.info(f'Deleted {deleted_count} objects for analysis_id {analysis_id} at {prefix}')
        else:
            logging.warning(f'No S3 objects found for analysis_id {analysis_id} at {prefix}')
    
    logging.info(f'Successfully deleted {total_deleted} objects across {len(analysis_ids)} analysis IDs')


def get_phenovar_http_conn():
    """Create HTTP connection to Phenovar server."""
    if config.phenovar_url.startswith('https'):
        conn = http.client.HTTPSConnection(phenovar_url_parts.hostname)
    else:
        conn = http.client.HTTPConnection(phenovar_url_parts.hostname, port=phenovar_url_parts.port)
    return conn


def get_phenovar_headers() -> dict:
    """Get HTTP headers for Phenovar API requests including auth token."""
    return {
        'Content-Type': 'application/json',
        'Authorization': f'Token {config.phenovar_api_token}'
    }


def parse_response(res, log_body=True):
    """Parse HTTP response and handle errors."""
    data = res.read()
    body = data.decode('utf-8')
    
    if log_body:
        logging.info(f'{res.status} - {body}')
    else:
        logging.info(f'{res.status}')
    
    if res.status != 200:
        raise AirflowFailException(f'Error from Phenovar API call: {body}')
    
    return body


def parse_response_json(res):
    """Parse HTTP response as JSON."""
    return json.loads(parse_response(res))


def submit_phenovar_analysis(payload: dict) -> dict:
    """
    Submit analysis request to Phenovar API.
    Returns: dict with keys 'task_id', 'message', 'status_url'
    """
    conn = get_phenovar_http_conn()
    headers = get_phenovar_headers()
    payload_json = json.dumps(payload).encode('utf-8')
    
    logging.info(f'Submitting Phenovar analysis: {payload_json.decode("utf-8")}')
    
    endpoint = phenovar_url_parts.path + '/phenovar3/rest_api/dxtablegenerator/generate/'
    conn.request("POST", endpoint, payload_json, headers)
    
    response = conn.getresponse()
    conn.close()
    
    return parse_response_json(response)


def check_phenovar_status(task_id: str) -> dict:
    """
    Check status of Phenovar task.
    Returns: dict with keys 'status', 'result' (if completed)
    """
    conn = get_phenovar_http_conn()
    headers = get_phenovar_headers()
    
    endpoint = f'{phenovar_url_parts.path}/phenovar3/rest_api/dxtablegenerator/check-status/?task_id={task_id}'
    logging.info(f'Checking Phenovar status for task_id: {task_id}')
    
    conn.request("GET", endpoint, "", headers)
    response = conn.getresponse()
    conn.close()
    
    return parse_response_json(response)


def download_phenovar_results(task_id: str) -> str:
    """
    Download Phenovar results JSON.
    Returns: JSON string of results
    """
    status_response = check_phenovar_status(task_id)
    
    if status_response.get('status') != 'SUCCESS':
        raise AirflowFailException(f'Cannot download results, status is {status_response.get("status")}')
    
    result_data = status_response.get('result')
    if not result_data:
        raise AirflowFailException('No result data in response')
    
    return json.dumps(result_data)
