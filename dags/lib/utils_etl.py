import json
from enum import Enum
from typing import Optional, List

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config
from lib.config import clin_import_bucket, config_file


class ClinAnalysis(Enum):
    GERMLINE = 'germline'
    SOMATIC_TUMOR_ONLY = 'somatic_tumor_only'
    SOMATIC_TUMOR_NORMAL = 'somatic_tumor_normal'


class ClinVCFSuffix(Enum):
    SNV_GERMLINE = '.hard-filtered.formatted.norm.VEP.vcf.gz'
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


def skip_import() -> str:
    return '{% if params.batch_id and params.batch_id|length and params.import == "yes" %}{% else %}yes{% endif %}'


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
        chromosome: Optional[str] = None) -> List[str]:
    arguments = [
            '--config', config_file,
            '--steps', steps,
            '--app-name', app_name,
    ]
    if entrypoint:
        arguments = [entrypoint] + arguments
    if batch_id:
        arguments = arguments + ['--batchId', batch_id]
    if chromosome:
        arguments = arguments + ['--chromosome', f'chr{chromosome}']
    return arguments
