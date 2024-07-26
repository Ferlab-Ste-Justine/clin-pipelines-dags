from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config

#TODO: remove any hard coded information, polish the code
class NextflowOperator(KubernetesPodOperator):
    def __init__(
        self,
        k8s_context: str,
        **kwargs,
    ) -> None:
        super().__init__(
            is_delete_operator_pod=True,
            in_cluster=config.k8s_in_cluster(k8s_context),
            config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.nextflow_image,
            **kwargs,
        )

    def execute(self, **kwargs):
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
            )
        ]
        
        #TODO: assume that all config files are packaged within the same config map?

        self.volumes = [
            k8s.V1Volume(
                name='nextflow-config',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name="nextflow", #TODO: Should be configurable
                ),
            ),
            k8s.V1Volume(
                name='nextflow-workspace',
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="cqgc-qa-nextflow-pvc") #TODO: Should be configurable
                #TODO: other settings for subpath?
            )
        ]

        #TODO: should not be hard coded
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='nextflow-config',
                mount_path='/opt/nextflow/config',
                read_only=True,
            ),
            k8s.V1VolumeMount(
                name='nextflow-workspace',
                mount_path="/mnt/workspace",
                read_only=True
            )
        ]

        self.working_dir = "/mnt/workspace" ##TODO: it does not work for now ... might not be supported through this api?

        super().execute(**kwargs)
