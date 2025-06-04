import kubernetes
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from lib import config
from lib.config import K8sContext


if (config.show_test_dags):

    with DAG(
        dag_id='test_kube_client_etl',
        start_date=datetime(2022, 1, 1),
        schedule=None,
    ) as dag:

        def _test_kube_client_etl():
            config.k8s_load_config(K8sContext.ETL)
            k8s_client = kubernetes.client.CoreV1Api()
            list = k8s_client.list_namespaced_pod(
                namespace=config.k8s_namespace,
                limit=10,
            )
            logging.info('pods:')
            for item in list.items:
                logging.info(f'{item.metadata.name} ({item.status.phase})')

        test_kube_client_etl = PythonOperator(
            task_id='test_kube_client_etl',
            python_callable=_test_kube_client_etl,
        )
