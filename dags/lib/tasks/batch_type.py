import logging
from typing import Dict, List

from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib.config import s3_conn_id, clin_import_bucket
from lib.utils_etl import metadata_exists, get_metadata_content, ClinSchema, ClinAnalysis, ClinVCFSuffix


def _validate_snv_vcf_files(s3: S3Hook, batch_id: str, snv_suffix: str):
    """
    Validate that SNV VCF files exist in Minio since they are not indicated in the metadata.
    """
    logging.info(f'Expecting SNV VCF(s) suffix: {snv_suffix}')

    has_valid_snv_vcf = False
    keys = s3.list_keys(clin_import_bucket, f'{batch_id}/')
    for key in keys:
        if key.endswith(snv_suffix):
            logging.info(f'Valid SNV VCF file: {key}')
            has_valid_snv_vcf = True

    if not has_valid_snv_vcf:
        raise AirflowFailException(f'No valid SNV VCF(s) found')


def _validate_cnv_vcf_files(metadata: dict, cnv_suffix: str):
    """
    Validate that CNV VCF files exist by checking if they are indicated in the metadata.
    """
    logging.info(f'Expecting CNV VCF(s) suffix: {cnv_suffix}')

    all_cnv_vcf_valid = True
    for analysis in metadata['analyses']:
        cnv_file = analysis.get('files', {}).get('cnv_vcf')
        if cnv_file:
            if cnv_file.endswith(cnv_suffix):
                logging.info(f'Valid CNV VCF file: {cnv_file}')
            else:
                logging.info(f'Invalid CNV VCF file: {cnv_file}')
                all_cnv_vcf_valid = False

    if not all_cnv_vcf_valid:
        raise AirflowFailException(f'Not all valid CNV VCF(s) found')


@task(task_id='detect_batch_type')
def detect(batch_id: str) -> Dict[str, str]:
    """
    Returns a dict where the key is the batch id and the value is the batch type.
    """
    clin_s3 = S3Hook(s3_conn_id)

    if metadata_exists(clin_s3, batch_id):
        # If the metadata file exists, it's either a GERMLINE or SOMATIC_TUMOR_ONLY analysis
        metadata = get_metadata_content(clin_s3, batch_id)
        submission_schema = metadata.get('submissionSchema', '')
        if submission_schema == ClinSchema.GERMLINE.value:
            batch_type = ClinAnalysis.GERMLINE.value
        elif submission_schema == ClinSchema.SOMATIC_TUMOR_ONLY.value:
            batch_type = ClinAnalysis.SOMATIC_TUMOR_ONLY.value
        else:
            raise AirflowFailException(f'Invalid submissionSchema: {submission_schema}')
    else:
        # If the metadata file doesn't exist, it's a SOMATIC_TUMOR_NORMAL analysis
        batch_type = ClinAnalysis.SOMATIC_TUMOR_NORMAL.value

    return {batch_id: batch_type}


def skip(batch_type: ClinAnalysis, batch_type_detected: bool,
         detect_batch_type_task_id: str = 'detect_batch_type') -> str:
    """
    Checks the return value of the detect_batch_type task. If it corresponds to the batch type passed in argument,
    it will return a string ('') that will be evaluated to False -- tasks won't be skipped. Otherwise, returns a string
    ('yes') that will be evaluated to True -- tasks will be skipped. This function has to return a string and not a bool
    since it uses Jinja Templating at runtime.

    If the bach type was not detected, it means the batch should not be skipped.
    """
    if batch_type_detected:
        return f"{{% if task_instance.xcom_pull(task_ids='{detect_batch_type_task_id}').values()|first == '{batch_type.value}' %}}" \
               "{% else %}yes{% endif %}"
    else:
        return ''  # Tasks won't be skipped


def any_in(targets: List[str], batch_types: List[Dict[str, str]]):
    """
    Macro for Jinja templating. Checks if there is at least one target type in a list of batch types.
    """
    batch_types_list = [batch_type for bt in batch_types for batch_id, batch_type in bt.items()]
    return any(target in batch_types_list for target in targets)


def skip_if_no_batch_in(target_batch_types: List[ClinAnalysis]) -> str:
    """
    Checks if at least one current batch type matches at least one of the target batch types. If there is a single
    match or if no batch_ids were passed, returns False so task won't be skipped. Otherwise, if there are no matches,
    returns True so task will be skipped.

    To use, pass macro any_in as user_defined_macros in DAG definition.
    """
    return f"{{% set targets = {[target.value for target in target_batch_types]} %}}" \
           "{% set batch_types = task_instance.xcom_pull(task_ids='detect_batch_type') %}" \
           "{% if not batch_types or any_in(targets, batch_types) %}{% else %}'yes'{% endif %}"


@task(task_id='validate_batch_type')
def validate(batch_id: str, batch_type: ClinAnalysis, skip: str = ''):
    if skip:
        raise AirflowSkipException()

    clin_s3 = S3Hook(s3_conn_id)
    metadata = get_metadata_content(clin_s3, batch_id) if metadata_exists(clin_s3, batch_id) else {}
    submission_schema = metadata.get('submissionSchema', '')

    if batch_type == ClinAnalysis.GERMLINE:
        if submission_schema != ClinSchema.GERMLINE.value:
            raise AirflowFailException(f'Invalid submissionSchema: {submission_schema}')

        logging.info(f'Schema: {submission_schema}')

        snv_vcf_suffix = ClinVCFSuffix.SNV_GERMLINE.value
        cnv_vcf_suffix = ClinVCFSuffix.CNV_GERMLINE.value

        _validate_snv_vcf_files(clin_s3, batch_id, snv_vcf_suffix)
        _validate_cnv_vcf_files(metadata, cnv_vcf_suffix)

    elif batch_type == ClinAnalysis.SOMATIC_TUMOR_ONLY:
        if submission_schema != ClinSchema.SOMATIC_TUMOR_ONLY.value:
            raise AirflowFailException(f'Invalid submissionSchema: {submission_schema}')

        logging.info(f'Schema: {submission_schema}')

        snv_vcf_suffix = ClinVCFSuffix.SNV_SOMATIC_TUMOR_ONLY.value
        cnv_vcf_suffix = ClinVCFSuffix.CNV_SOMATIC_TUMOR_ONLY.value

        _validate_snv_vcf_files(clin_s3, batch_id, snv_vcf_suffix)
        _validate_cnv_vcf_files(metadata, cnv_vcf_suffix)

    elif batch_type == ClinAnalysis.SOMATIC_TUMOR_NORMAL:
        if metadata:
            raise AirflowFailException(f'Metadata file should not exist for Somatic Tumor Normal')

        snv_vcf_suffix = ClinVCFSuffix.SNV_SOMATIC_TUMOR_NORMAL.value
        _validate_snv_vcf_files(clin_s3, batch_id, snv_vcf_suffix)
