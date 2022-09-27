import kubernetes
from airflow.models.baseoperator import BaseOperator
from airflow.utils.trigger_rule import TriggerRule
from lib import config


class K8sDeploymentPauseOperator(BaseOperator):

    def __init__(
        self,
        k8s_context: str,
        deployment: str,
        trigger_rule=TriggerRule.NONE_FAILED,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.k8s_context = k8s_context
        self.deployment = deployment

    def execute(self, context):
        config.k8s_load_config(self.k8s_context)
        k8s_client = kubernetes.client.AppsV1Api()
        k8s_client.patch_namespaced_deployment(
            name=self.deployment,
            namespace=config.k8s_namespace,
            body={'spec': {'paused': True}},
        )
