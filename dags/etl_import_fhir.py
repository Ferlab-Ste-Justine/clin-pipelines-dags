from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.config import K8sContext, env
from lib.operators.fhir import FhirOperator
from lib.operators.fhir_csv import FhirCsvOperator
from lib.operators.k8s_deployment_restart import K8sDeploymentRestartOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.operators.wait import WaitOperator
from lib.slack import Slack
from lib.tasks.params_validate import validate_color
from lib.utils_etl import color, skip_if_param_not

with DAG(
    dag_id='etl_import_fhir',
    start_date=datetime(2022, 1, 1),
    schedule=None,
    params={
        'fhir': Param('no', enum=['yes', 'no']),
        'csv': Param('no', enum=['yes', 'no']),
        'color': Param('', type=['null', 'string']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
) as dag:

    params_validate = validate_color(color())

    def fhir() -> str:
        return '{{ params.fhir or "" }}'

    def csv() -> str:
        return '{{ params.csv or "" }}'
    
    trigger_import_hpo = TriggerDagRunOperator(
        task_id='import_hpo',
        trigger_dag_id='etl_import_hpo_terms',
        wait_for_completion=True,
        skip=skip_if_param_not(fhir(), "yes"),  # only if we are importing FHIR
        conf={
            'color': color(),
        }
    )

    ig_publish = FhirOperator(
        task_id='ig_publish',
        name='etl-import-fhir-ig-publish',
        k8s_context=K8sContext.DEFAULT,
        skip=skip_if_param_not(fhir(), "yes"),
        color=color(),
    )

    wait_30s = WaitOperator(
        task_id='wait_30s',
        time='30s',
        skip=skip_if_param_not(fhir(), "yes"),
    )

    csv_import = FhirCsvOperator(
        task_id='csv_import',
        name='etl-import-fhir-csv-import',
        k8s_context=K8sContext.DEFAULT,
        skip=skip_if_param_not(csv(), "yes"),
        color=color(),
        arguments=['-f', f'{env}.yml'],
    )

    restart_qlin_me = K8sDeploymentRestartOperator(
        task_id='restart_qlin_me',
        k8s_context=K8sContext.DEFAULT,
        deployment='qlin-me-hybrid',
    )

    restart_forms = K8sDeploymentRestartOperator(
        task_id='restart_forms',
        k8s_context=K8sContext.DEFAULT,
        deployment='forms',
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion,
    )

    params_validate >> trigger_import_hpo >> ig_publish >> wait_30s >> csv_import >> [restart_qlin_me, restart_forms] >> slack
