from contextlib import contextmanager
from dataclasses import dataclass
from unittest.mock import patch

from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import pytest


@dataclass(order=True)
class SampleEntry:
    sample: str
    familyId: str
    vcf: str


def test_prepare_should_include_expected_data(prepare, nextflow_bucket, clin_minio, mock_clinical):
    clinical_df = pd.DataFrame([
        # (BAT1, SRA1) trio analysis
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '1',  'is_proband': True,  'father_aliquot_id': '2', 'mother_aliquot_id': '3', 'cnv_vcf_germline_urls': ['s3a://1.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '2',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://2.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '3',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://3.vcf']},

        # (BAT1, SRA2) trio analysis with siblings
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '11',  'is_proband': True,  'father_aliquot_id': '22', 'mother_aliquot_id': "33", 'cnv_vcf_germline_urls': ['s3a://11.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '22',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://22.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '33',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://33.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '44',  'is_proband': False, 'father_aliquot_id': '22', 'mother_aliquot_id': "33", 'cnv_vcf_germline_urls': ['s3a://44.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '55',  'is_proband': False, 'father_aliquot_id': '22', 'mother_aliquot_id': "33", 'cnv_vcf_germline_urls': ['s3a://55.vcf']},

        # (BAT1, SRA3) Duo
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA3', 'aliquot_id': '111',  'is_proband': True,  'father_aliquot_id': None, 'mother_aliquot_id': "222", 'cnv_vcf_germline_urls': ['s3a://111.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA3', 'aliquot_id': '222',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://222.vcf']},

        # (BAT1, SRA4) Solo: Should not be included
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA4', 'aliquot_id': '1111',  'is_proband': True,  'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://1111.vcf']},

        # (BAT2, SRA5) Duo with no CNVs: Should not be included
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA5', 'aliquot_id': '11111',  'is_proband': True,  'father_aliquot_id': None, 'mother_aliquot_id': "22222", 'cnv_vcf_germline_urls': None},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA5', 'aliquot_id': '22222',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': None},

        # (BAT2, SRA6) : Should not be included
        {'batch_id': 'BAT2', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA6', 'aliquot_id': '111111',  'is_proband': True,  'father_aliquot_id': None, 'mother_aliquot_id': "222222", 'cnv_vcf_germline_urls': ['s3a://111111.vcf']},
        {'batch_id': 'BAT2', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA6', 'aliquot_id': '222222',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://222222.vcf']}
    ])

    with mock_clinical(clinical_df):
        output_url = prepare.function({'SRA1', 'SRA2', 'SRA3', 'SRA4', 'SRA5'}, 'testhash')

        expected_key = 'svclustering_parental_origin/input/testhash.samplesheet.csv'
        expected_url = f"s3://{nextflow_bucket}/{expected_key}"
        assert output_url == expected_url

        uploaded_data = _read_samplesheet(expected_key, nextflow_bucket, clin_minio)
        assert len(uploaded_data) == 10

        # aliquot_id, analysis_id, vcf
        expected_data = sorted([
            # (BAT1, SRA1) trio analysis
            SampleEntry(sample='1', familyId='SRA1', vcf='s3://1.vcf'),
            SampleEntry(sample='2', familyId='SRA1', vcf='s3://2.vcf'),
            SampleEntry(sample='3', familyId='SRA1', vcf='s3://3.vcf'),

            # (BAT1, SRA2) trio analysis with siblings
            SampleEntry(sample='11', familyId='SRA2', vcf='s3://11.vcf'),
            SampleEntry(sample='22', familyId='SRA2', vcf='s3://22.vcf'),
            SampleEntry(sample='33', familyId='SRA2', vcf='s3://33.vcf'),
            SampleEntry(sample='44', familyId='SRA2', vcf='s3://44.vcf'),
            SampleEntry(sample='55', familyId='SRA2', vcf='s3://55.vcf'),

            # (BAT1, SRA3) Duo
            SampleEntry(sample='111', familyId='SRA3', vcf='s3://111.vcf'),
            SampleEntry(sample='222', familyId='SRA3', vcf='s3://222.vcf')
        ])
        assert uploaded_data == expected_data


def test_prepare_should_remove_duplicated_samples_in_qa(clean_up_clin_minio, clin_minio, mock_clinical):
    clinical_df = pd.DataFrame([
        # (BAT1, SRA1, trio)
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '1',  'is_proband': True,  'father_aliquot_id': '2', 'mother_aliquot_id': '3', 'cnv_vcf_germline_urls': ['s3a://1.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '2',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://2.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '3',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://3.vcf']},

        # (BAT1, SRA2) duplicated aliquot id (also in SRA1):
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '3',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://3.vcf']},

        # (BAT1, SRA2): trio to make sure analysis SRA2 is retained
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '11',  'is_proband': True,  'father_aliquot_id': '22', 'mother_aliquot_id': "33", 'cnv_vcf_germline_urls': ['s3a://11.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '22',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://22.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '33',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://33.vcf']}
    ])

    with (
        mock_clinical(clinical_df),
        patch("lib.config.env", "qa"),  # simulating that we are in qa environment
    ):
        from dags.lib.tasks.nextflow.svclustering_parental_origin import prepare
        from lib.config_nextflow import nextflow_bucket

        output_url = prepare.function({'SRA1', 'SRA2'}, 'testhash')

        expected_key = 'svclustering_parental_origin/input/testhash.samplesheet.csv'
        expected_url = f"s3://{nextflow_bucket}/{expected_key}"
        assert output_url == expected_url

        uploaded_data = _read_samplesheet(expected_key, nextflow_bucket, clin_minio)
        assert len(uploaded_data) == 6
        expected_data = sorted([
            # analysis SRA1, including aliquot 3 (kept)
            SampleEntry(sample='1', familyId='SRA1', vcf='s3://1.vcf'),
            SampleEntry(sample='2', familyId='SRA1', vcf='s3://2.vcf'),
            SampleEntry(sample='3', familyId='SRA1', vcf='s3://3.vcf'),

            # analysis SRA2 without aliquot 3 (drop because duplicated)
            SampleEntry(sample='11', familyId='SRA2', vcf='s3://11.vcf'),
            SampleEntry(sample='22', familyId='SRA2', vcf='s3://22.vcf'),
            SampleEntry(sample='33', familyId='SRA2', vcf='s3://33.vcf'),
        ])
        assert uploaded_data == expected_data


def test_prepare_should_throw_exception_if_duplicated_samples_outside_qa(prepare, mock_clinical):
    clinical_df = pd.DataFrame([
        # (BAT1, SRA1, trio)
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '1',  'is_proband': True,  'father_aliquot_id': '2', 'mother_aliquot_id': '3', 'cnv_vcf_germline_urls': ['s3a://1.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '2',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://2.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '3',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://3.vcf']},

        # (BAT1, SRA2) duplicated aliquot id (also in SRA1):
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '3',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://3.vcf']},

        # (BAT1, SRA2): trio to make sure analysis SRA2 is retained
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '11',  'is_proband': True,  'father_aliquot_id': '22', 'mother_aliquot_id': "33", 'cnv_vcf_germline_urls': ['s3a://11.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '22',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://22.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '33',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://33.vcf']}

    ])
    with mock_clinical(clinical_df):
        with pytest.raises(AirflowFailException, match=r"Duplicate aliquot_ids: \['3'\]"):
            prepare.function({'SRA1', 'SRA2'}, 'testhash')


def test_prepare_should_not_write_file_if_no_cnv_urls(nextflow_bucket, prepare, clin_minio, mock_clinical):
    clinical_df = pd.DataFrame([
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '1',  'is_proband': True,  'father_aliquot_id': None, 'mother_aliquot_id': "2", 'cnv_vcf_germline_urls': None},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '2',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': None},
    ])

    with mock_clinical(clinical_df):
        output_url = prepare.function({'SRA1'}, 'testhash')

        assert not output_url
        assert not clin_minio.list_prefixes(nextflow_bucket)  # no files were uploaded


def test_prepare_should_handle_same_analysis_in_2_batches(nextflow_bucket, prepare, clin_minio, mock_clinical):

    clinical_df = pd.DataFrame([
        # Batch 1: Incomplete trio
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '1',  'is_proband': True,  'father_aliquot_id': '3', 'mother_aliquot_id': '2', 'cnv_vcf_germline_urls': ['s3a://1.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '2',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://2.vcf']},

        # Batch 2: Father of the trio, received in a later batch
        {'batch_id': 'BAT2', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '3',  'is_proband': False,  'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://3.vcf']},
    ])
    with mock_clinical(clinical_df):
        output_url = prepare.function({'SRA1'}, 'testhash')

        expected_key = 'svclustering_parental_origin/input/testhash.samplesheet.csv'
        expected_url = f"s3://{nextflow_bucket}/{expected_key}"
        assert output_url == expected_url

        uploaded_data = _read_samplesheet(expected_key, nextflow_bucket, clin_minio)
        assert len(uploaded_data) == 3
        expected_data = sorted([
            SampleEntry(sample='1', familyId='SRA1', vcf='s3://1.vcf'),
            SampleEntry(sample='2', familyId='SRA1', vcf='s3://2.vcf'),
            SampleEntry(sample='3', familyId='SRA1', vcf='s3://3.vcf'),
        ])
        assert uploaded_data == expected_data


def test_prepare_should_handle_multiple_batches(nextflow_bucket, prepare, clin_minio, mock_clinical):
    clinical_df = pd.DataFrame([
        # Batch 1: Incomplete trio
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '1',  'is_proband': True,  'father_aliquot_id': None, 'mother_aliquot_id': '2', 'cnv_vcf_germline_urls': ['s3a://1.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '2',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://2.vcf']},

        # Batch 2: Father of the trio, received in a later batch
        {'batch_id': 'BAT2', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '3',  'is_proband': True,  'father_aliquot_id': '4', 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://3.vcf']},
        {'batch_id': 'BAT2', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA2', 'aliquot_id': '4',  'is_proband': False,  'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://4.vcf']},
    ])
    with mock_clinical(clinical_df):
        output_url = prepare.function({'SRA1', 'SRA2'}, 'testhash')

        expected_key = 'svclustering_parental_origin/input/testhash.samplesheet.csv'
        expected_url = f"s3://{nextflow_bucket}/{expected_key}"
        assert output_url == expected_url

        uploaded_data = _read_samplesheet(expected_key, nextflow_bucket, clin_minio)
        assert len(uploaded_data) == 4
        expected_data = sorted([
            SampleEntry(sample='1', familyId='SRA1', vcf='s3://1.vcf'),
            SampleEntry(sample='2', familyId='SRA1', vcf='s3://2.vcf'),
            SampleEntry(sample='3', familyId='SRA2', vcf='s3://3.vcf'),
            SampleEntry(sample='4', familyId='SRA2', vcf='s3://4.vcf'),
        ])
        assert uploaded_data == expected_data


def test_prepare_retain_only_geba_samples(nextflow_bucket, prepare, clin_minio, mock_clinical):
    clinical_df = pd.DataFrame([
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'TEBA', 'analysis_id': 'SRA1', 'aliquot_id': '111',  'is_proband': True,  'father_aliquot_id': None, 'mother_aliquot_id': "222", 'cnv_vcf_germline_urls': ['s3a://111.vcf']},
        {'batch_id': 'BAT1', 'bioinfo_analysis_code': 'GEBA', 'analysis_id': 'SRA1', 'aliquot_id': '222',  'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'cnv_vcf_germline_urls': ['s3a://222.vcf']},
    ])

    with mock_clinical(clinical_df):
        output_url = prepare.function({'SRA1'}, 'testhash')

        assert not output_url
        assert not clin_minio.list_prefixes(nextflow_bucket)  # no files were uploaded


@pytest.fixture
def nextflow_bucket(mock_airflow_variables):
    from lib.config_nextflow import nextflow_bucket
    return nextflow_bucket


@pytest.fixture
def prepare(mock_airflow_variables, clean_up_clin_minio):
    from dags.lib.tasks.nextflow.svclustering_parental_origin import prepare
    return prepare


@pytest.fixture
def mock_clinical(clin_minio):
    @contextmanager
    def _patch(clinical_df):
        with (
            patch('lib.utils_etl_tables.to_pandas', return_value=clinical_df),
            patch('airflow.providers.amazon.aws.hooks.s3.S3Hook', return_value=clin_minio)
        ):
            yield
    return _patch


def _read_samplesheet(key: str, bucket: str, clin_minio: S3Hook) -> list[SampleEntry]:
    data = clin_minio.read_key(key, bucket).strip()

    lines = [line.split(',') for line in data.splitlines()]
    if not lines:
        return []
    header = lines[0]
    return sorted([SampleEntry(**dict(zip(header, line))) for line in lines[1:]])
