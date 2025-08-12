from airflow.models import MappedOperator
from airflow.models.dag import DAG


def test_group_has_all_tasks():
    from lib.groups.ingest.ingest_fhir import ingest_fhir
    with DAG(dag_id='test_dag') as dag:
        ingest_fhir(
            batch_ids=['batch1', 'batch2'],
            color='',
            skip_all='',
            skip_import='',
            skip_batch='',
            spark_jar=''
        )

    expected_tasks = {
        'fhir.prepare_expand_batch_ids',
        'fhir.fhir_import',
        'fhir.fhir_export',
        'fhir.fhir_normalize',
        'fhir.fhir_enrich_clinical'
    }
    task_ids = {task.task_id for task in dag.tasks}
    assert task_ids == expected_tasks, f"Missing tasks: {expected_tasks - task_ids}"

    fhir_import_task = dag.get_task('fhir.fhir_import')
    assert isinstance(fhir_import_task, MappedOperator)
    #assert fhir_import_task.expand_input.value == {'batch_id': ['batch1', 'batch2']}
