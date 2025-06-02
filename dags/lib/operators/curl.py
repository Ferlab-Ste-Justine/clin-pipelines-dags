from typing import List

from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config
from lib.config import env


class CurlOperator(KubernetesPodOperator):

    template_fields = KubernetesPodOperator.template_fields + (
        'skip',
    )

    def __init__(
        self,
        k8s_context: str,
        skip: bool = False,
        skip_fail_env: List[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            on_finish_action='delete_pod',
            in_cluster=config.k8s_in_cluster(k8s_context),
            config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.curl_image,
            **kwargs,
        )
        self.skip = skip
        self.skip_fail_env = skip_fail_env

    def execute(self, **kwargs):

        if self.skip:
            raise AirflowSkipException()

        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.volumes = [
            k8s.V1Volume(
                name='ingress-ca-certificate',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=config.ca_certificates,
                    default_mode=0o555,
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='ingress-ca-certificate',
                mount_path='/opt/ingress-ca',
                read_only=True,
            ),
        ]

        try:
            super().execute(**kwargs)
        except AirflowException:
            if self.skip_fail_env is not None and env in self.skip_fail_env:
                raise AirflowSkipException()
            else:
                raise AirflowFailException('Curl request failed')
