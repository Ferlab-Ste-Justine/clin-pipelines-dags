from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime
from lib import config
from lib.config import K8sContext


if (config.show_test_dags):

    with DAG(
        dag_id='test_pod_operator_etl',
        start_date=datetime(2022, 1, 1),
        schedule=None,
    ) as dag:

        test_pod_operator_etl = KubernetesPodOperator(
            task_id='test_pod_operator_etl',
            name='test-pod-operator-etl',
            on_finish_action='delete_pod',
            in_cluster=config.k8s_in_cluster(K8sContext.ETL),
            config_file=config.k8s_config_file(K8sContext.ETL),
            cluster_context=config.k8s_cluster_context(K8sContext.ETL),
            namespace=config.k8s_namespace,
            image='alpine',
            cmds=['echo', 'hello'],
            arguments=[],
        )
