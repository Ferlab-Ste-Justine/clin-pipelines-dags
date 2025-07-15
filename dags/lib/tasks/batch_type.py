import logging
from collections import defaultdict
from typing import Dict, List, Optional

from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib.config import clin_import_bucket, s3_conn_id
from lib.datasets import enriched_clinical
from lib.utils_etl import (ClinAnalysis, ClinSchema, ClinVCFSuffix,
                           get_metadata_content, metadata_exists)


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
# Note that we restrict amount of activate mapped tasks per DAG to avoid memory issues and delta lake connection problems.
@task.virtualenv(task_id='detect_batch_type', requirements=["deltalake===0.24.0"], inlets=[enriched_clinical], max_active_tis_per_dag=1)
def detect(batch_id: str = None, batch_ids: List[str] = None, analysis_ids: List[str] = None, allowMultipleIdentifierTypes: bool = False) -> Dict[str, str]:
    """
    Returns a dict where the key is the batch id or the analysis id and the value are the analysis types.

    If `batch_ids` is provided and the analysis type cannot be determined from the `enriched_clinical` table,
    the function will attempt to infer the analysis type from the metadata file. If the metadata file does
    not exist, the analysis type will default to `SOMATIC_TUMOR_NORMAL`.

    The possible analysis types (formerly referred to as batch type) are:
        - GERMLINE
        - SOMATIC_TUMOR_ONLY
        - SOMATIC_TUMOR_NORMAL
    """
    import logging
    from collections import defaultdict

    from airflow.exceptions import AirflowFailException
    from lib.tasks.batch_type import (_detect_types_from_enrich_clinical,
                                      _detect_types_from_metadata_file)
    from lib.utils_etl import ClinAnalysis

    logger = logging.getLogger(__name__)

    batch_ids = batch_ids if batch_ids else [batch_id] if batch_id and batch_id != "" else []
    analysis_ids = analysis_ids if analysis_ids else []

    if len(batch_ids) > 0 and len(analysis_ids) > 0 and not allowMultipleIdentifierTypes:
        raise AirflowFailException("Only one of batch_id or analysis_ids can be provided")

    if len(batch_ids) == 0 and len(analysis_ids) == 0:
        # we can't raise an airflow skip exception here from a virtual task
        logger.warning("Neither batch_id or analysis_ids have been provided")
        return defaultdict(str)

    batch_ids_to_type = _detect_types_from_enrich_clinical(
        identifier_column="batch_id",
        identifiers=batch_ids,
        must_exist=False
    )

    missing_batch_ids = set(batch_ids) - set(batch_ids_to_type.keys())
    if missing_batch_ids:
        logger.info(f"Unable to infer batch type for batch ID {missing_batch_ids} from the enriched clinical table. Falling back to metadata file.")
        batch_ids_to_type.update(_detect_types_from_metadata_file(missing_batch_ids))

    analysis_ids_to_type = _detect_types_from_enrich_clinical(
        identifier_column="analysis_id",
        identifiers=analysis_ids,
        must_exist=True
    )

    # validate and keep only one type per identifier
    identifier_to_type = defaultdict(str)
    for batch_id, types in batch_ids_to_type.items():
        if len(types) > 1:  # should never happen
            raise AirflowFailException(f"Batch ID {batch_id} has multiple analysis types: {types}")
        identifier_to_type[batch_id] = types[0]
    for analysis_id, types in analysis_ids_to_type.items():
        if analysis_id in identifier_to_type: # for some reason a batch_id and analysis have the same value
            raise AirflowFailException(f"Duplicated identifier between batch_id and analysis_id: {analysis_id}")
        # remove SOMATIC_TUMOR_NORMAL if part of the types, somatic normal can only be imported via batch_id
        types = [t for t in types if t != ClinAnalysis.SOMATIC_TUMOR_NORMAL.value]
        if len(types) > 1: # should never happen unless in the future we add new analysis types
            raise AirflowFailException(f"Sequencing: {analysis_id} has multiple analysis types: {types}")
        identifier_to_type[analysis_id] = types[0]

    # in case the DAG explicitly request one unique analysis type allowed
    all_types = set(identifier_to_type.values())
    if len(all_types) > 1 and not allowMultipleIdentifierTypes:
        raise AirflowFailException(f"DAG doesn't allow multiple analysis types: {all_types}")
    
    return identifier_to_type
   

def _detect_types_from_enrich_clinical(identifier_column: str, identifiers: List[str], must_exist=True) -> Dict[str, List[str]]:
    """
    Returns a dictionary mapping each identifier to its corresponding analysis type.

    The possible analysis types (formerly referred to as batch type) are:
     - GERMLINE
     - SOMATIC_TUMOR_ONLY
     - SOMATIC_TUMOR_NORMAL

    The identifiers provided must match the values in the specified `identifier_column`
    of the `enriched_clinical` table.
    """
    import logging
    from collections import defaultdict

    from airflow.exceptions import AirflowFailException
    from lib.datasets import enriched_clinical
    from lib.utils_etl import BioinfoAnalysisCode
    from lib.utils_etl_tables import to_pandas
    from pandas import DataFrame

    logger = logging.getLogger(__name__)

    df: DataFrame = (
        to_pandas(enriched_clinical.uri)
        .filter([identifier_column, "bioinfo_analysis_code"])
        .set_index(identifier_column)
    )
    distinct_pairs_df = df[df.index.isin(identifiers)]

    identifier_to_codes = defaultdict(list)
    identifiers_with_unknown_codes = defaultdict(list)
    identifier_to_types = defaultdict(list)
    for _id, code in distinct_pairs_df.itertuples():
        if  code not in identifier_to_codes[_id]: # avoid duplicates
            identifier_to_codes[_id].append(code)
            if code not in BioinfoAnalysisCode:
                identifiers_with_unknown_codes[_id].append(code)
            else:
                identifier_to_types[_id].append(BioinfoAnalysisCode(code).to_analysis_type())

    logger.info(f"Identifiers to analysis type: {identifier_to_codes}")

    if identifiers_with_unknown_codes:
        raise AirflowFailException(f"Some identifiers map to unknown codes: {identifiers_with_unknown_codes}")

    if must_exist:
        missing_identifiers = set(identifiers) - set(identifier_to_codes.keys())
        if missing_identifiers:
            raise AirflowFailException(f"IDs of type: {identifier_column} not found in clinical data: {missing_identifiers}")

    return identifier_to_types


def _detect_types_from_metadata_file(batch_ids: List[str]) -> Dict[str, List[str]]:
    """
    Returns a dict where the key is the batch id and the value is the analysis type.

    The possible analysis types (formerly referred to as batch type) are:
     - GERMLINE
     - SOMATIC_TUMOR_ONLY
     - SOMATIC_TUMOR_NORMAL
    """
    clin_s3 = S3Hook(s3_conn_id)

    identifier_to_types = defaultdict(list)

    for batch_id in batch_ids:
        batch_type = None
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
        identifier_to_types[batch_id] = [batch_type]

    logging.info(f"Batch IDs to analysis type: {identifier_to_types}")
    return identifier_to_types


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
def validate(batch_id: str, analysis_ids: list, batch_type: ClinAnalysis, skip: str = ''):
    if skip:
        raise AirflowSkipException()
           
    if (not batch_id or batch_id == "") and len(analysis_ids) == 0:
         raise AirflowFailException('Neither batch_id or analysis_ids have been provided')
    
    if batch_id:
        clin_s3 = S3Hook(s3_conn_id)
        _metadata_exists = metadata_exists(clin_s3, batch_id)
        metadata = get_metadata_content(clin_s3, batch_id) if metadata_exists else {}
        submission_schema = metadata.get('submissionSchema', '<no metadata found>')

        if batch_type == ClinAnalysis.GERMLINE:
            if _metadata_exists and submission_schema != ClinSchema.GERMLINE.value:
                raise AirflowFailException(f'Invalid submissionSchema: {submission_schema}')

            logging.info(f'Schema: {submission_schema}')

            snv_vcf_suffix = ClinVCFSuffix.SNV_GERMLINE.value
            cnv_vcf_suffix = ClinVCFSuffix.CNV_GERMLINE.value

            _validate_snv_vcf_files(clin_s3, batch_id, snv_vcf_suffix)
            _validate_cnv_vcf_files(metadata, cnv_vcf_suffix)

        elif _metadata_exists and batch_type == ClinAnalysis.SOMATIC_TUMOR_ONLY:
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

    if len(analysis_ids) > 0:
        raise AirflowSkipException('Validation for analysis_ids is not implemented yet.')