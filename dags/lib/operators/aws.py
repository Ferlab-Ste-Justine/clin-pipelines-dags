from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from lib import config


class AwsOperator(KubernetesPodOperator):

    def __init__(
        self,
        k8s_context: str,
        **kwargs,
    ) -> None:
        super().__init__(
            on_finish_action='delete_pod',
            in_cluster=config.k8s_in_cluster(k8s_context),
            config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.aws_image,
            **kwargs,
        )

    def execute(self, **kwargs):
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='AWS_ACCESS_KEY_ID',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='download-s3-credentials',
                        key='S3_ACCESS_KEY',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_SECRET_ACCESS_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='download-s3-credentials',
                        key='S3_SECRET_KEY',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_DEFAULT_REGION',
                value='regionone',
            ),
        ]
        self.volumes = [
            k8s.V1Volume(
                name='minio-ca-certificate',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=config.minio_certificate,
                    default_mode=0o555,
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='minio-ca-certificate',
                mount_path='/opt/minio-ca',
                read_only=True,
            ),
        ]

        super().execute(**kwargs)
