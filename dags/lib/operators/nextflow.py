import logging
import re

from airflow.exceptions import AirflowSkipException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
    KubernetesPodOperator
from kubernetes.client import models as k8s

from lib import config
from lib.operators.utils import utils_pod

logger = logging.getLogger(__name__)


class NextflowOperator(KubernetesPodOperator):
    """
    Custom operator to run nextflow within a kubernetes pod.

    See `test_nextflow_operator.py` for an usage example.

    Nextflow is launched within a persistent volume to ensure
    the persistence of logs, checkpoints, and history files.
    Each execution gets a unique launch directory to avoid
    conflicts. The pod's working directory is set to this
    launch directory.

    Minio credentials are provided via a Kubernetes secret,
    and nextflow configuration file(s) are injected through
    a Kubernetes configmap, mounted at `/root/nextflow/config/`.
    """

    template_fields = KubernetesPodOperator.template_fields + ('skip',)

    def __init__(
        self,
        k8s_context: str,
        skip: bool = False,
        **kwargs
    ) -> None:
        super().__init__(
            is_delete_operator_pod=False,
            in_cluster=config.k8s_in_cluster(k8s_context),
            config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.nextflow_image,
            service_account_name=config.nextflow_service_account,
            **kwargs
        )

        self.config_map_name = config.nextflow_config_map
        self.minio_secret_name = config.nextflow_minio_secret
        self.minio_access_key_property = config.nextflow_minio_access_key_property
        self.minio_secret_key_property = config.nextflow_minio_secret_key_property
        self.persistent_volume_claim_name = config.nextflow_pvc
        self.persistent_volume_sub_path = config.nextflow_pv_sub_path
        self.skip = skip

        # Where nextflow will write intermediate outputs. This is different
        # from the launch directory, i.e. where the nextflow command is
        # executed.
        self.nextflow_working_dir = config.nextflow_working_dir

    def execute(self, context, **kwargs):
        if self.skip:
            raise AirflowSkipException()

        persistent_volume_mount_path = "/mnt/workspace"

        self.env_vars = [
            k8s.V1EnvVar(
                name="AWS_ACCESS_KEY_ID",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=self.minio_secret_name,
                        key=self.minio_access_key_property
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_SECRET_ACCESS_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=self.minio_secret_name,
                        key=self.minio_secret_key_property
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name="NXF_WORK",
                value=self.nextflow_working_dir
            ),

            k8s.V1EnvVar(
                name="NXF_EXECUTOR",
                value="k8s"
            )
        ]

        self.volumes = [
            k8s.V1Volume(
                name='nextflow-config',
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=self.config_map_name
                ),
            ),
            k8s.V1Volume(
                name='nextflow-workspace',
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name=self.persistent_volume_claim_name
                )
            )
        ]

        self.volume_mounts = [
            k8s.V1VolumeMount(
                name='nextflow-config',
                mount_path='/root/nextflow/config',
                read_only=True,
            ),
            k8s.V1VolumeMount(
                name='nextflow-workspace',
                mount_path=persistent_volume_mount_path,
                sub_path=self.persistent_volume_sub_path
            )
        ]

        # As far as we know, the KubernetesPodOperator does not provide
        # a direct attribute to set the working directory. Therefore, we
        # configure it within the container specification in attribute
        # full_pod_spec.
        pod_working_dir = _get_pod_working_dir(
            persistent_volume_mount_path,
            context
        )
        logger.info("Setting pod working directory to %s", pod_working_dir)
        self.full_pod_spec = utils_pod.add_working_dir_to_pod_spec(
            pod_working_dir,
            existing_pod_spec=self.full_pod_spec,
            container_name=self.base_container_name
        )

        super().execute(context, **kwargs)

# ---------------- #
# HELPER FUNCTIONS #
# ---------------- #


def _get_pod_working_dir(base_path, context):
    ti = context["ti"]  # the task instance
    sanitized_run_id = _sanitize_run_id(context["run_id"])

    # Will be empty for non-map tasks
    map_index_part = f"_{ti.map_index}" if ti.map_index >= 0 else ""

    return f"{base_path}/{ti.dag_id}/{ti.task_id}{map_index_part}/{sanitized_run_id}_trial{ti.try_number}"


# Remove characters `+` and `:` from the run id
def _sanitize_run_id(run_id):
    return re.sub(r'[+:]', '', run_id)
