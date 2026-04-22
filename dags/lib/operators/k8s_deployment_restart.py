import logging
import time
from datetime import datetime

import kubernetes
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from lib import config


class K8sDeploymentRestartOperator(BaseOperator):

    template_fields = ('deployment', 'skip')

    def __init__(
        self,
        k8s_context: str,
        deployment: str,
        skip: bool = False,
        wait_seconds: int = 0,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.k8s_context = k8s_context
        self.deployment = deployment
        self.skip = skip
        self.wait_seconds = wait_seconds

    def execute(self, context):

        if self.skip:
            raise AirflowSkipException()

        now = str(datetime.utcnow().isoformat('T') + 'Z')
        config.k8s_load_config(self.k8s_context)
        k8s_client = kubernetes.client.AppsV1Api()
        patched = k8s_client.patch_namespaced_deployment(
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

        if self.wait_seconds > 0:
            self._wait_for_rollout(k8s_client, patched.metadata.generation)

    def _wait_for_rollout(self, k8s_client, trigger_generation: int) -> None:
        """Poll until the deployment has rolled out the new spec or wait_seconds elapses."""
        deadline = time.monotonic() + self.wait_seconds
        poll_interval = 3

        while time.monotonic() < deadline:
            dep = k8s_client.read_namespaced_deployment_status(
                name=self.deployment,
                namespace=config.k8s_namespace,
            )
            desired = dep.spec.replicas or 0
            status = dep.status
            observed_generation = status.observed_generation or 0
            updated = status.updated_replicas or 0
            available = status.available_replicas or 0
            unavailable = status.unavailable_replicas or 0

            if (
                observed_generation >= trigger_generation
                and updated >= desired
                and available >= desired
                and unavailable == 0
            ):
                logging.info(
                    f'Deployment {self.deployment} rolled out '
                    f'(generation={observed_generation}, available={available}/{desired})'
                )
                return

            logging.info(
                f'Waiting for {self.deployment} rollout: '
                f'generation={observed_generation}/{trigger_generation}, '
                f'updated={updated}/{desired}, available={available}/{desired}, '
                f'unavailable={unavailable}'
            )
            time.sleep(poll_interval)

        raise AirflowFailException(
            f'Deployment {self.deployment} did not become ready within {self.wait_seconds}s'
        )
