import logging
from typing import Dict, List, Optional

from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib.config import s3_conn_id, clin_import_bucket
from lib.datasets import enriched_clinical
from lib.utils_etl import (
    metadata_exists,
    get_metadata_content,
    ClinSchema,
    ClinAnalysis,
    ClinVCFSuffix
)


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


# Preserving the old function name and task ID for backward compatibility.
# In the future, we may consider renaming this to remove references to the batch concept.
@task.virtualenv(task_id='detect_batch_type', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
def detect(batch_id: Optional[str] = None, sequencing_ids: Optional[List[str]] = None) -> Dict[str, str]:
    """
    Returns a dict where the key is the batch id or the sequencing id and the value is the analysis type.

    Here batch_id and sequencing_ids are mutually exclusive. If both are provided, an exception will be raised.

    If a `batch_id` is provided and the analysis type cannot be determined from the `enriched_clinical` table,
    the function will attempt to infer the analysis type from the metadata file. If the metadata file does
    not exist, the analysis type will default to `SOMATIC_TUMOR_NORMAL`.

    The possible analysis types (formerly referred to as batch type) are:
        - GERMLINE
        - SOMATIC_TUMOR_ONLY
        - SOMATIC_TUMOR_NORMAL
    """
    import logging
    from airflow.exceptions import AirflowFailException
    from lib.tasks.batch_type import _detect_type_from_enrich_clinical, _detect_type_from_metadata_file

    logger = logging.getLogger(__name__)

    if batch_id and sequencing_ids:
        raise AirflowFailException("Only one of batch_id or sequencing_ids can be provided")

    if not (batch_id or sequencing_ids):
        raise AirflowFailException("Either batch_id or sequencing_ids must be provided")

    identifier_to_type = _detect_type_from_enrich_clinical(
        identifier_column="batch_id" if batch_id else "service_request_id",
        identifiers=[batch_id] if batch_id else sequencing_ids,
        must_exist=bool(sequencing_ids)
    )
    if not identifier_to_type and batch_id:
        logger.info(f"Unable to infer batch type for batch ID {batch_id} from the enriched clinical table. Falling back to metadata file.")
        return _detect_type_from_metadata_file(batch_id)
    else:
        return identifier_to_type


def _detect_type_from_enrich_clinical(identifier_column, identifiers, must_exist=True):
    """
    Returns a dictionary mapping each identifier to its corresponding analysis type.

    The possible analysis types (formerly referred to as batch type) are:
     - GERMLINE
     - SOMATIC_TUMOR_ONLY
     - SOMATIC_TUMOR_NORMAL

    The identifiers provided must match the values in the specified `identifier_column`
    of the `enriched_clinical` table.
    """
    from collections import defaultdict
    from airflow.exceptions import AirflowFailException
    from pandas import DataFrame
    from lib.datasets import enriched_clinical
    from lib.utils_etl import BioinfoAnalysisCode
    from lib.utils_etl_tables import to_pandas

    df: DataFrame = (
        to_pandas(enriched_clinical.uri)
        .filter([identifier_column, "bioinfo_analysis_code"])
        .set_index(identifier_column)
    )
    distinct_pairs_df = df[df.index.isin(identifiers)].drop_duplicates()

    identifier_to_codes = defaultdict(list)
    for _id, code in distinct_pairs_df.itertuples():
        identifier_to_codes[_id].append(code)

    identifiers_with_multiple_codes = {k: v for k, v in identifier_to_codes.items() if len(v) > 1}
    if identifiers_with_multiple_codes:
        raise AirflowFailException(f"Multiple bioinfo_analysis_code found for some identifiers: {identifiers_with_multiple_codes}")

    identifiers_with_unknown_codes = {k: v for k, v in identifier_to_codes.items() if v[0] not in BioinfoAnalysisCode}
    if identifiers_with_unknown_codes:
        raise AirflowFailException(f"Some identifiers map to unknown codes: {identifiers_with_unknown_codes}")

    if must_exist:
        missing_identifiers = set(identifiers) - set(identifier_to_codes.keys())
        if missing_identifiers:
            raise AirflowFailException(f"IDs not found in clinical data: {missing_identifiers}")

    identifier_to_type = {k: BioinfoAnalysisCode[v[0]].to_analysis_type() for k, v in identifier_to_codes.items()}
    return identifier_to_type


def _detect_type_from_metadata_file(batch_id: str) -> Dict[str, str]:
    """
    Returns a dict where the key is the batch id and the value is the analysis type.

    The possible analysis types (formerly referred to as batch type) are:
     - GERMLINE
     - SOMATIC_TUMOR_ONLY
     - SOMATIC_TUMOR_NORMAL
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
