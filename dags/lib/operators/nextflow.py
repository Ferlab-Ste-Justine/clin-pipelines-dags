from pathlib import Path
import re

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from lib import config
from lib.operators.utils import utils_pod



class NextflowOperator(KubernetesPodOperator):
    def __init__(
        self,
        k8s_context: str,
        persistent_volume_claim_name: str,
        persistent_volume_mount_path: str = "/mnt/workspace",
        **kwargs
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

        self.persistent_volume_claim_name = persistent_volume_claim_name
        self.persistent_volume_mount_path = persistent_volume_mount_path

        

    def execute(self, context, **kwargs):
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
        
        self.volumes = [
            k8s.V1Volume(
                name='nextflow-config',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name="nextflow"
                ),
            ),
            k8s.V1Volume(
                name='nextflow-workspace',
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=self.persistent_volume_claim_name) 
                #TODO: other settings for subpath?
            )
        ]

        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='nextflow-config',
                mount_path='/opt/nextflow/config',
                read_only=True,
            ),
            k8s.V1VolumeMount(
                name='nextflow-workspace',
                mount_path= self.persistent_volume_mount_path,
            )
        ]

        # To ensure the persistence of Nextflow logs, checkpoints, and history files, we launch Nextflow
        # within the persistent volume. Each Nextflow execution is assigned a unique launch directory to prevent
        # conflicts between concurrent runs. We set the pod working directory to this launch directory.
        # Note: As far as we know, the KubernetesPodOperator does not provide a direct attribute to set the working
        # directory. Therefore, we configure it within the container specification.
        working_dir = _generate_working_dir(self.persistent_volume_mount_path, context)
        self.full_pod_spec = utils_pod.add_working_dir_to_pod_spec(working_dir, existing_pod_spec=self.full_pod_spec)

        super().execute(context, **kwargs)


def _generate_working_dir(base_path, context):
    ti = context["ti"]
    run_id = context["run_id"]
    sanitized_run_id = re.sub(r'[+:]', '', run_id)

    map_index_part = f"_{ti.map_index}" if ti.map_index >= 0 else ""
    dag_task_base_path =  Path(base_path) / ti.dag_id / f"{ti.task_id}{map_index_part}"
    run_info = f"{sanitized_run_id}_trial{ti.try_number}"

    return str(dag_task_base_path / run_info)
