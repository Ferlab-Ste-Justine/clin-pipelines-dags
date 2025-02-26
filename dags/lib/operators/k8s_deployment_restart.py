from datetime import datetime

import kubernetes
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from lib import config


class K8sDeploymentRestartOperator(BaseOperator):

    template_fields = ('deployment', 'skip')

    def __init__(
        self,
        k8s_context: str,
        deployment: str,
        skip: bool = False,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.k8s_context = k8s_context
        self.deployment = deployment
        self.skip = skip

    def execute(self, context):

        if self.skip:
            raise AirflowSkipException()

        now = str(datetime.utcnow().isoformat('T') + 'Z')
        config.k8s_load_config(self.k8s_context)
        k8s_client = kubernetes.client.AppsV1Api()
        k8s_client.patch_namespaced_deployment(
            name=self.deployment,
            namespace=config.k8s_namespace,
            body={
                'spec': {
                    'template': {
                        'metadata': {
                            'annotations': {
                                'kubectl.kubernetes.io/restartedAt': now
                            }
                        }
                    }
                }
            },
        )
