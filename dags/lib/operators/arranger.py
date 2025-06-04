from airflow.exceptions import AirflowSkipException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from lib import config


class ArrangerOperator(KubernetesPodOperator):

    template_fields = KubernetesPodOperator.template_fields + (
        'skip',
    )

    def __init__(
        self,
        k8s_context: str,
        skip: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            on_finish_action='delete_pod',
            in_cluster=config.k8s_in_cluster(k8s_context),
            config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.arranger_image,
            **kwargs,
        )
        self.skip = skip

    def execute(self, **kwargs):
        if self.skip:
            raise AirflowSkipException()
            
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='NODE_ENV',
                value='production',
            ),
            k8s.V1EnvVar(
                name='NODE_EXTRA_CA_CERTS',
                value='/opt/ingress-ca/ca.crt',
            ),
            k8s.V1EnvVar(
                name='SERVICE_ACCOUNT_CLIENT_SECRET',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='keycloak-client-system-credentials',
                        key='client-secret',
                    ),
                ),
            ),
        ]
        self.env_from = [
            k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(
                    name='arranger-keycloak-configs',
                ),
            ),
            k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(
                    name='arranger-es-configs',
                ),
            ),
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(
                    name='arranger-session-secret',
                ),
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

        super().execute(**kwargs)
