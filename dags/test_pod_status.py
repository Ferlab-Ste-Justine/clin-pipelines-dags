import kubernetes
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib.etl import config
from lib.k8s.config import k8s_load_config


with DAG(
    dag_id='test_pod_status',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    def _test_pod_status():
        k8s_load_config()
        k8s_client = kubernetes.client.CoreV1Api()
        list = k8s_client.list_namespaced_pod(
            namespace=config.k8s_namespace,
            limit=10,
        )
        logging.info('pods:')
        for item in list.items:
            logging.info(f'{item.metadata.name} ({item.status.phase})')

    test_pod_status = PythonOperator(
        task_id='pod_status',
        python_callable=_test_pod_status,
    )

    test_pod_status