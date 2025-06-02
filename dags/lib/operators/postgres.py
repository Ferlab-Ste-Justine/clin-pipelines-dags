from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config
from lib.config import env
from lib.utils import join


class PostgresOperator(KubernetesPodOperator):

    template_fields = KubernetesPodOperator.template_fields + (
        'color',
    )

    def __init__(
        self,
        k8s_context: str,
        color: str = '',
        **kwargs,
    ) -> None:
        super().__init__(
            on_finish_action='delete_pod',
            in_cluster=config.k8s_in_cluster(k8s_context),
            config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.postgres_image,
            **kwargs,
        )
        self.color = color

    def execute(self, **kwargs):

        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='PGUSER',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=f'cqgc-{env}-postgres-credentials',
                        key='PGUSER',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='PGPASSWORD',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=f'cqgc-{env}-postgres-credentials',
                        key='PGPASSWORD',
                    ),
                ),
            ),
        ]
        self.env_from = [
            k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(
                    name=join(
                        '-', ['fhir-server', self.color, 'db-connection'],
                    ),
                ),
            ),
        ]
        self.volumes = [
            k8s.V1Volume(
                name='ca-certificate',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=f'cqgc-{env}-postgres-ca-cert',
                    default_mode=0o555,
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='ca-certificate',
                mount_path='/opt/ca',
                read_only=True,
            ),
        ]

        super().execute(**kwargs)
