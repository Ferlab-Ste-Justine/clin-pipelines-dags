from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from lib.config import env, K8sContext
from lib.operators.fhir import FhirOperator
from lib.operators.fhir_csv import FhirCsvOperator
from lib.operators.wait import WaitOperator
from lib.slack import Slack
from lib.tasks.params_validate import validate_color
from lib.utils_etl import color

with DAG(
    dag_id='etl_import_fhir',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    params={
        'color': Param('', type=['null', 'string']),
    },
    default_args={
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    params_validate = validate_color(color())

    ig_publish = FhirOperator(
        task_id='ig_publish',
        name='etl-import-fhir-ig-publish',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
    )

    wait_30s = WaitOperator(
        task_id='wait_30s',
        time='30s',
    )

    csv_import = FhirCsvOperator(
        task_id='csv_import',
        name='etl-import-fhir-csv-import',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        arguments=['-f', f'{env}.yml'],
        on_success_callback=Slack.notify_dag_completion,
    )

    params_validate >> ig_publish >> wait_30s >> csv_import
