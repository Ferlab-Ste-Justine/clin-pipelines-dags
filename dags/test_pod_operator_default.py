from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime
from lib import config


with DAG(
    dag_id='test_pod_operator_default',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
) as dag:

    test_pod_operator_default = KubernetesPodOperator(
        task_id='test_pod_operator_default',
        is_delete_operator_pod=False,
        in_cluster=False,
        # config_file='/home/airflow/.kube/config',
        cluster_context='airflow-cluster.qa.cqgc@cluster.qa.cqgc',
        namespace=config.k8s_namespace,
        name='test-pod-operator-default',
        image='alpine',
        cmds=['echo', 'hello'],
        arguments=[],
    )

    test_pod_operator_default