"""
etl_import_franklin DAG test cases
"""

DAG_NAME = "etl_import_franklin"


def test_dag_is_importable(dag_bag):
    dag = dag_bag.get_dag(DAG_NAME)
    assert dag is not None


def test_dag_has_all_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_NAME)
    expected_tasks = {
        'get_batch_ids',
        'get_analysis_ids',
        'params_validate',
        'fhir.prepare_expand_batch_ids',
        'fhir.fhir_import',
        'fhir.fhir_export',
        'fhir.fhir_normalize',
        'fhir.fhir_enrich_clinical',
        'detect_batch_type',
        'franklin_validate',
        'get_all_analysis_ids',
        'reset_franklin_data',
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
