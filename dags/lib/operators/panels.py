from airflow.exceptions import AirflowSkipException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config
from lib.config import env, env_url
from lib.utils import join


class PanelsOperator(KubernetesPodOperator):

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
            image=config.panels_image,
            **kwargs,
        )
        self.skip = skip

    def execute(self, **kwargs):

        if self.skip:
            raise AirflowSkipException()

        self.cmds = [
            '/opt/entrypoint/entrypoint.sh',
            'java', '-cp', 'app.jar',
        ]
        self.image_pull_policy = 'Always'
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='AWS_ACCESS_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='s3-files-processing-credentials',
                        key='S3_ACCESS_KEY',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_SECRET_KEY',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='s3-files-processing-credentials',
                        key='S3_SECRET_KEY',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='AWS_ENDPOINT',
                value='https://s3.cqgc.hsj.rtss.qc.ca',
            ),
            k8s.V1EnvVar(
                name='AWS_DATALAKE_BUCKET_NAME',
                value=f'cqgc-{env}-app-datalake',
            ),
            k8s.V1EnvVar(
                name='AWS_PUBLIC_BUCKET_NAME',
                value=f'cqgc-{env}-app-public',
            ),
        ]
        self.volumes = [
            k8s.V1Volume(
                name='entrypoint',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name='spark-jobs-entrypoint',
                    default_mode=0o555,
                ),
            ),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='entrypoint',
                mount_path='/opt/entrypoint',
            ),
        ]

        super().execute(**kwargs)
