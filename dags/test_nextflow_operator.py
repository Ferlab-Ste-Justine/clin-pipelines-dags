from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from lib.config import K8sContext, nextflow_service_account
from lib.operators.nextflow import NextflowOperator

with DAG(
    dag_id="test_nextflow_operator",
    start_date=datetime(2022, 1, 1),
    schedule=None
):

   NextflowOperator(
        task_id='nextflow-hello',
        name='etl-nextflow-hello',
        k8s_context = K8sContext.ETL,
        service_account_name =  nextflow_service_account,
        arguments = [
            "nextflow",
            "run",
            "nextflow-io/hello",
            "-c",
            "/opt/nextflow/config/nextflow.config"
        ]
    )