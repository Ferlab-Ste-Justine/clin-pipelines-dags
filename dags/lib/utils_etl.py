import json
from enum import Enum
import logging
from typing import Dict, List, Optional, Set

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
from lib.tasks.params import get_param
from lib import config
from lib.config import clin_import_bucket, config_file, env, Env, es_url


class ClinAnalysis(Enum):
    GERMLINE = 'germline'
    SOMATIC_TUMOR_ONLY = 'somatic_tumor_only'
    SOMATIC_TUMOR_NORMAL = 'somatic_tumor_normal'


class ClinVCFSuffix(Enum):
    SNV_GERMLINE = '.norm.VEP.vcf.gz'
    SNV_GERMLINE_LEGACY = f'.hard-filtered.formatted{SNV_GERMLINE}'
    SNV_SOMATIC_TUMOR_ONLY = '.dragen.WES_somatic-tumor_only.hard-filtered.norm.VEP.vcf.gz'
    SNV_SOMATIC_TUMOR_NORMAL = '.vcf.gz'
    CNV_GERMLINE = '.cnv.vcf.gz'
    CNV_SOMATIC_TUMOR_ONLY = '.dragen.WES_somatic-tumor_only.cnv.vcf.gz'


class ClinSchema(Enum):
    GERMLINE = 'CQGC_Germline'
    SOMATIC_TUMOR_ONLY = 'CQGC_Exome_Tumeur_Seul'


class BioinfoAnalysisCode(Enum):
    GEBA = 'GEBA'
    TEBA = 'TEBA'
    TNEBA = 'TNEBA'

    def to_analysis_type(self) -> str:
        mapping = {
            BioinfoAnalysisCode.GEBA: ClinAnalysis.GERMLINE.value,
            BioinfoAnalysisCode.TEBA: ClinAnalysis.SOMATIC_TUMOR_ONLY.value,
            BioinfoAnalysisCode.TNEBA: ClinAnalysis.SOMATIC_TUMOR_NORMAL.value
        }
        return mapping[self]


def batch_id() -> str:
    return '{{ params.batch_id or "" }}'


def release_id(index: Optional[str] = None) -> str:
    if not index:
        return '{{ params.release_id or "" }}'

    return f"{{{{ task_instance.xcom_pull(task_ids='get_release_ids.get_{index}_release_id') }}}}"


def spark_jar() -> str:
    return '{{ params.spark_jar or "" }}'


def obo_parser_spark_jar() -> str:
    return '{{ params.obo_parser_spark_jar or "' + config.obo_parser_spark_jar + '" }}'


def color(prefix: str = '') -> str:
    return '{% if params.color and params.color|length %}' + prefix + '{{ params.color }}{% endif %}'


def get_index_of_alias(alias_name: str):
    response = requests.get(f'{es_url}/_cat/aliases/{alias_name}?format=json', verify=False)
    data = response.json()
    index_name = data[0]['index'] if response.ok and len(data) > 0 else None

    if index_name:
        logging.info(f'The index matching alias "{alias_name}" is: "{index_name}"')
    else:
        logging.warning(f'No index found for alias "{alias_name}"')

    return index_name


def get_color(index_name: str):
    if "blue" in index_name:
        return 'blue'
    if "green" in index_name:
        return 'green'
    return None


def get_current_color() -> str:
    if env != Env.QA:
        raise Exception('get_current_color should only be used in QA environment')

    gene_centric_index = get_index_of_alias('clin_qa_gene_centric')
    color = get_color(gene_centric_index)

    if color:
        logging.info(f'Current color is: {color}')
    else:
        logging.info('No current color found')
    return color


def skip_import(batch_param_name: str = 'batch_id') -> str:
    return f'{{% if params.{batch_param_name} and params.{batch_param_name}|length and params.import == "yes" %}}{{% else %}}yes{{% endif %}}'


def skip_batch() -> str:
    return '{% if params.batch_id and params.batch_id|length %}{% else %}yes{% endif %}'


def default_or_initial(batch_param_name: str = 'batch_id') -> str:
    return f'{{% if params.{batch_param_name} and params.{batch_param_name}|length %}}default{{% else %}}initial{{% endif %}}'


def skip_notify(batch_param_name: str = 'batch_id') -> str:
    return f'{{% if params.{batch_param_name} and params.{batch_param_name}|length and params.notify == "yes" %}}{{% else %}}yes{{% endif %}}'


def skip_if_param_not(param_template, value) -> str:
    return f'{{% if ({param_template}) and ({param_template}|length) and ({param_template}) == "{value}" %}}{{% else %}}yes{{% endif %}}'.replace(
        '{{', '').replace('}}', '')


def skip(cond1: str, cond2: str) -> str:
    """
    Skips the task if one of the conditions is True.

    Since both conditions are Jinja-templated strings evaluated at runtime, this function concatenates the two strings.
    An empty string means False (task not skipped) and a non-empty string means True (task skipped). Therefore,
    concatenating both strings produces the same result as a boolean OR operator.
    """
    return cond1 + cond2


def metadata_exists(clin_s3: S3Hook, batch_id: str) -> bool:
    metadata_path = f'{batch_id}/metadata.json'
    return clin_s3.check_for_key(metadata_path, clin_import_bucket)


def batch_folder_exists(clin_s3: S3Hook, batch_id: str) -> bool:
    metadata_path = f'{batch_id}'
    return clin_s3.check_for_prefix(prefix=metadata_path, delimiter="/", bucket_name=clin_import_bucket)


def get_metadata_content(clin_s3, batch_id) -> dict:
    metadata_path = f'{batch_id}/metadata.json'
    file_obj = clin_s3.get_key(metadata_path, clin_import_bucket)
    return json.loads(file_obj.get()['Body'].read().decode('utf-8'))


def get_group_id(prefix: str, batch_id: str) -> str:
    return prefix + '_' + batch_id.replace('.', '')  # '.' not allowed


# Constructs arguments using the standard mainargs-based ETL interface
def build_etl_job_arguments(
        app_name: str,
        entrypoint: Optional[str] = None,
        steps: str = "default",
        batch_id: Optional[str] = None,
        batch_id_deprecated: Optional[bool] = False,
        analysis_ids: Optional[list[str]] = None,
        bioinfo_analysis_code: Optional[str] = None,
        chromosome: Optional[str] = None) -> List[str]:
    arguments = [
            '--config', config_file,
            '--steps', steps,
            '--app-name', app_name,
    ]
    if entrypoint:
        arguments = [entrypoint] + arguments
    if not batch_id_deprecated and batch_id and batch_id != '':
        arguments = arguments + ['--batchId', batch_id]
    if batch_id_deprecated and analysis_ids:
        for analysis_id in analysis_ids:
            arguments += ['--analysisId', analysis_id]
        if bioinfo_analysis_code:
            arguments = arguments + ['--bioinfoAnalysisCode', bioinfo_analysis_code]
    if chromosome:
        arguments = arguments + ['--chromosome', f'chr{chromosome}']
    return arguments


@task(task_id='get_ingest_dag_configs_by_batch_id')
def get_ingest_dag_configs_by_batch_id(batch_id: str) -> dict:
    context = get_current_context()
    params = context["params"]
    return {
        'batch_id': batch_id,
        'analysis_ids': None,
        'color': params['color'],
        'import': params['import'],
        'spark_jar': params['spark_jar']
    }


@task(task_id='get_germline_analysis_ids')
def get_germline_analysis_ids(all_batch_types: Dict[str, str], analysis_ids: List[str]) -> List[str]:
    return _get_analysis_ids_compatible_with_type(
        all_batch_types=all_batch_types,
        analysis_ids=analysis_ids,
        analysisType=ClinAnalysis.GERMLINE.value
    )


@task(task_id='get_ingest_dag_configs_by_analysis_ids')
def get_ingest_dag_configs_by_analysis_ids(all_batch_types: Dict[str, str], analysis_ids: List[str], analysisType: str) -> dict:
    context = get_current_context()
    params = context["params"]

    # try regroup analysis ids and generate a config of etl_ingest for each analysis type
    analysis_ids_compatible_with_type = _get_analysis_ids_compatible_with_type(all_batch_types, analysis_ids, analysisType)

    if len(analysis_ids_compatible_with_type) == 0:
        return None  # No analysis ids found for that analysis type

    return {
        'batch_id': None,
        'analysis_ids': analysis_ids_compatible_with_type,
        'color': params['color'],
        'import': get_param(params, 'import', 'no'),
        'spark_jar': params['spark_jar'],
    }


@task(task_id='get_job_hash')
def get_job_hash(analysis_ids: Set[str], skip: str) -> str:
    """
    Generate a unique hash for the job using the analysis IDs associated to the input sequencing IDs. The hash is used to name the input samplesheet file and the output directory in the nextflow post-processing pipeline.
    """
    if skip:
        raise AirflowSkipException("Skipping job hash generation task.")
    from lib.utils import urlsafe_hash
    return urlsafe_hash(analysis_ids, length=14)  # 14 is safe for up to 1B hashes


def _get_analysis_ids_compatible_with_type(all_batch_types: Dict[str, str], analysis_ids: List[str], analysisType: str) -> List[str]:
    analysis_ids_compatible_with_type = []
    for identifier, type in all_batch_types.items():
        if analysisType == type and identifier in analysis_ids:
            analysis_ids_compatible_with_type.append(identifier)
    return analysis_ids_compatible_with_type
