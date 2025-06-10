"""
etl_import_franklin DAG test cases
"""
import pytest

from tests.conftest import RESOURCES_DIR

DAG_NAME = "etl_import_franklin"


def test_dag_is_importable(dag_bag):
    dag = dag_bag.get_dag(DAG_NAME)
    assert dag is not None


def test_dag_has_all_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_NAME)
    expected_tasks = {
        'params',
        'create.create_analyses',
        'create.vcf_to_analyses',
        'create.group_families',
        'update.api_sensor',
        'update.download_results',
        'update.clean_up_clin',
        'update.clean_up_franklin',
        'slack'
    }
    task_ids = {task.task_id for task in dag.tasks}
    assert task_ids == expected_tasks, f"Missing tasks: {expected_tasks - task_ids}"


@pytest.mark.vpn
@pytest.mark.slow
def test_dag_runs(mock_airflow_variables,
                  clin_minio,
                  franklin_s3,
                  disable_get_franklin_http_conn_ssl):
    from dags.etl_import_franklin import dag as etl_import_franklin
    from lib.config import clin_import_bucket, clin_datalake_bucket

    # Set up test
    batch_id = 'BATCH_1'
    metadata_file_name = 'metadata.json'
    metadata_local_path = RESOURCES_DIR / metadata_file_name
    metadata_s3_key = f'{batch_id}/{metadata_file_name}'

    solo_proband_aliquot_id = '1'
    solo_vcf_key = f'{batch_id}/{solo_proband_aliquot_id}.case.hard-filtered.formatted.norm.VEP.vcf.gz'

    family_id = 'FM_FRK_0001'
    family_proband_aliquot_id = '2'
    family_mother_aliquot_id = '3'
    family_father_aliquot_id = '4'
    family_vcf_key = f'{batch_id}/{family_proband_aliquot_id}.case.hard-filtered.formatted.norm.VEP.vcf.gz'

    # Load metadata file from resource folder
    clin_minio.load_file(
        filename=metadata_local_path,
        key=metadata_s3_key,
        bucket_name=clin_import_bucket,
        replace=True
    )

    # Load VCF files
    for vcf in [solo_vcf_key, family_vcf_key]:
        clin_minio.load_string(
            string_data='Mock VCF file content',
            key=vcf,
            bucket_name=clin_import_bucket,
            replace=True
        )

    # Run DAG
    dag_run = etl_import_franklin.test(
        run_conf={'batch_id': batch_id},
    )

    assert dag_run is not None, "DAG run failed to start"
    assert dag_run.state == 'success', f"DAG run did not complete successfully, state: {dag_run.state}"

    output_prefix = f'raw/landing/franklin/batch_id={batch_id}'
    output_keys = clin_minio.list_keys(
        bucket_name=clin_datalake_bucket,
        prefix=output_prefix
    )
    assert len(output_keys) == 5  # 1 solo + 1 trio (3 aliquots + 1 family analysis)
    assert all(key.endswith('analysis.json') for key in output_keys)
    # Solo analysis
    assert any(key.startswith(f'{output_prefix}/family_id=null/aliquot_id={solo_proband_aliquot_id}/analysis_id=') for key in output_keys)
    # Proband analysis
    assert any(key.startswith(f'{output_prefix}/family_id={family_id}/aliquot_id={family_proband_aliquot_id}/analysis_id=') for key in output_keys)
    # Mother analysis
    assert any(key.startswith(f'{output_prefix}/family_id={family_id}/aliquot_id={family_mother_aliquot_id}/analysis_id=') for key in output_keys)
    # Father analysis
    assert any(key.startswith(f'{output_prefix}/family_id={family_id}/aliquot_id={family_father_aliquot_id}/analysis_id=') for key in output_keys)
    # Family analysis
    assert any(key.startswith(f'{output_prefix}/family_id={family_id}/aliquot_id=null/analysis_id=') for key in output_keys)
