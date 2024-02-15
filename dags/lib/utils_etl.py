import json
from enum import Enum

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib.config import clin_import_bucket


class ClinAnalysis(Enum):
    GERMLINE = 'germline'
    SOMATIC_TUMOR_ONLY = 'somatic_tumor_only'
    SOMATIC_TUMOR_NORMAL = 'somatic_tumor_normal'


class ClinVCFSuffix(Enum):
    SNV_GERMLINE = '.hard-filtered.formatted.norm.VEP.vcf.gz'
    SNV_SOMATIC_TUMOR_ONLY = '.dragen.WES_somatic-tumor_only.hard-filtered.norm.VEP.vcf.gz'
    SNV_SOMATIC_TUMOR_NORMAL = '*.vcf.gz'
    CNV_GERMLINE = '.cnv.vcf.gz'
    CNV_SOMATIC_TUMOR_ONLY = '.dragen.WES_somatic-tumor_only.cnv.vcf.gz'


class ClinSchema(Enum):
    GERMLINE = 'CQGC_Germline'
    SOMATIC_TUMOR_ONLY = 'CQGC_Exome_Tumeur_Seul'


def batch_id() -> str:
    return '{{ params.batch_id or "" }}'


def release_id() -> str:
    return '{{ params.release_id }}'


def spark_jar() -> str:
    return '{{ params.spark_jar or "" }}'


def color(prefix: str = '') -> str:
    return '{% if params.color and params.color|length %}' + prefix + '{{ params.color }}{% endif %}'


def skip_import() -> str:
    return '{% if params.batch_id and params.batch_id|length and params.import == "yes" %}{% else %}yes{% endif %}'


def skip_batch() -> str:
    return '{% if params.batch_id and params.batch_id|length %}{% else %}yes{% endif %}'


def default_or_initial() -> str:
    return '{% if params.batch_id and params.batch_id|length and params.import == "yes" %}default{% else %}initial{% endif %}'


def skip_notify() -> str:
    return '{% if params.batch_id and params.batch_id|length and params.notify == "yes" %}{% else %}yes{% endif %}'


def metadata_exists(clin_s3: S3Hook, batch_id: str) -> bool:
    metadata_path = f'{batch_id}/metadata.json'
    return clin_s3.check_for_key(metadata_path, clin_import_bucket)


def get_metadata_content(clin_s3, batch_id) -> dict:
    metadata_path = f'{batch_id}/metadata.json'
    file_obj = clin_s3.get_key(metadata_path, clin_import_bucket)
    return json.loads(file_obj.get()['Body'].read().decode('utf-8'))


def get_dag_config() -> dict:
    return {
        'batch_id': batch_id(),
        'release_id': release_id(),
        'color': '{{ params.color }}',
        'import': '{{ params.import }}',
        'notify': '{{ params.notify }}',
        'spark_jar': spark_jar()
    }
