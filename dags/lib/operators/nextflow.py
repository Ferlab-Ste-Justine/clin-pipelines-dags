import copy
from dataclasses import dataclass, field
import logging
import re
from typing import Optional, List, Type
from typing_extensions import Self

from airflow.exceptions import AirflowSkipException
from airflow.utils.context import Context
from kubernetes.client import models as k8s

from lib.operators.base_kubernetes import (
    BaseConfig,
    BaseKubernetesOperator,
    ConfigMap,
    required)
from lib.operators.utils import utils_pod

logger = logging.getLogger(__name__)


class NextflowOperator(BaseKubernetesOperator):
    """
    Custom operator to run nextflow within a kubernetes pod.

    See `test_nextflow_operator.py` and `lib/tasks/nextflow.py`
    for example usage.

    Nextflow is launched within a persistent volume to ensure
    the persistence of logs, checkpoints, and history files.
    Each execution gets a unique launch directory to avoid
    conflicts. The pod's working directory is set to this
    launch directory.

    Minio credentials are provided via a Kubernetes secret,
    and nextflow configuration file(s) are injected through
    Kubernetes configmaps.
    """

    template_fields = [*BaseKubernetesOperator.template_fields,  'skip']

    def __init__(
        self,
        minio_credentials_secret_name: str,
        minio_credentials_secret_access_key: str,
        minio_credentials_secret_secret_key: str,
        persistent_volume_claim_name: str,
        persistent_volume_sub_path: str,
        persistent_volume_mount_path: str,
        nextflow_working_dir: str,
        skip: bool = False,
        config_maps: Optional[List[ConfigMap]] = None,
        **kwargs
    ) -> None:

        super().__init__(
            **kwargs
        )
        self.minio_credentials_secret_name = minio_credentials_secret_name
        self.minio_credentials_secret_access_key = minio_credentials_secret_access_key
        self.minio_credentials_secret_secret_key = minio_credentials_secret_secret_key

        self.persistent_volume_claim_name = persistent_volume_claim_name
        self.persistent_volume_sub_path = persistent_volume_sub_path
        self.persistent_volume_mount_path = persistent_volume_mount_path

        # Where nextflow will write intermediate outputs. This is different
        # from the pod working directory.
        self.nextflow_working_dir = nextflow_working_dir
        self.skip = skip
        self.config_maps = config_maps if config_maps else []

    def execute(self, context: Context, **kwargs):

        if self.skip:
            raise AirflowSkipException()

        self.env_vars = [
            k8s.V1EnvVar(
                name="NXF_WORK",
                value=self.nextflow_working_dir
            ),

            k8s.V1EnvVar(
                name="NXF_EXECUTOR",
                value="k8s"
            ),
            k8s.V1EnvVar(
                name="AWS_ACCESS_KEY_ID",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=self.minio_credentials_secret_name,
                        key=self.minio_credentials_secret_access_key
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_SECRET_ACCESS_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name=self.minio_credentials_secret_name,
                        key=self.minio_credentials_secret_secret_key
                    ),
                )
            )
        ]

        # config maps volumes
        self.volumes = [
            k8s.V1Volume(
                name=cm.name,
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=cm.name
                )
            )
            for cm in self.config_maps
        ]

        # persistent volume
        self.volumes.append(
            k8s.V1Volume(
                name='persistent-volume',
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name=self.persistent_volume_claim_name
                )
            )
        )

        # config maps volume mounts
        self.volume_mounts = [
            k8s.V1VolumeMount(
                name=cm.name,
                mount_path=cm.mount_path
            )
            for cm in self.config_maps
        ]

        # persistent volume mount
        self.volume_mounts.append(
            k8s.V1VolumeMount(
                name='persistent-volume',
                mount_path=self.persistent_volume_mount_path,
                sub_path=self.persistent_volume_sub_path
            )
        )

        # As far as we know, the KubernetesPodOperator does not provide a
        # direct attribute to set the working directory. Therefore, we
        # configureit within the container specification in attribute
        # full_pod_spec.
        pod_working_dir = _get_pod_working_dir(
              self.persistent_volume_mount_path,
              context
        )

        logger.info("Setting pod working directory to %s", pod_working_dir)
        self.full_pod_spec = utils_pod.add_working_dir_to_pod_spec(
            pod_working_dir,
            existing_pod_spec=self.full_pod_spec,
            container_name=self.base_container_name
        )

        super().execute(context, **kwargs)


@dataclass
class NextflowPipeline:
    """
    Represents a nextflow pipeline to be executed in a nextflow pod.
    """
    url: Optional[str] = None
    revision: Optional[str] = None
    config_maps: List[ConfigMap] = field(default_factory=list)
    config_files: List[str] = field(default_factory=list)
    params_file: Optional[str] = None

    def with_url(self, new_url: str) -> Self:
        c = copy.copy(self)
        c.url = new_url
        return c

    def with_revision(self, new_revision: str) -> Self:
        c = copy.copy(self)
        c.revision = new_revision
        return c

    def with_params_file(self, new_params_file: str) -> Self:
        c = copy.copy(self)
        c.params_file = new_params_file
        return c

    def append_config_maps(self, *new_config_maps) -> Self:
        c = copy.copy(self)
        c.config_maps = [*self.config_maps, *new_config_maps]
        return c

    def append_config_files(self, *new_config_files) -> Self:
        c = copy.copy(self)
        c.config_files = [*self.config_files, *new_config_files]
        return c


@dataclass
class NextflowOperatorConfig(BaseConfig):

    minio_credentials_secret_name: str = required()
    minio_credentials_secret_access_key: str = required()
    minio_credentials_secret_secret_key: str = required()
    persistent_volume_claim_name: str = required()
    persistent_volume_sub_path: str = required()
    persistent_volume_mount_path: str = required()
    nextflow_working_dir: str = required()
    skip: bool = False

    config_maps: List[str] = field(default_factory=list)

    def with_config_maps(self, new_config_maps: List[ConfigMap]) -> Self:
        c = copy.copy(self)
        c.config_maps = new_config_maps
        return c

    def extend_config_maps(self, *new_config_maps) -> Self:
        c = copy.copy(self)
        c.config_maps = [*self.config_maps, *new_config_maps]
        return c

    def operator(self,
                 class_to_instantiate: Type[NextflowOperator] = NextflowOperator,
                 **kwargs) -> NextflowOperator:
        return super().build_operator(
            class_to_instantiate=class_to_instantiate,
            **kwargs
        )

    def partial(self,
                class_to_instantiate: Type[NextflowOperator] = NextflowOperator,
                **kwargs):
        return super().partial(
            class_to_instantiate=class_to_instantiate,
            **kwargs
        )


# ---------------- #
# HELPER FUNCTIONS #
# ---------------- #


def _get_pod_working_dir(base_path: str, context: Context) -> str:
    ti = context["ti"]  # the task instance
    sanitized_run_id = _sanitize_run_id(context["run_id"])

    # Will be empty for non-map tasks
    map_index_part = f"_{ti.map_index}" if ti.map_index >= 0 else ""

    return f"{base_path}/{ti.dag_id}/{ti.task_id}{map_index_part}/{sanitized_run_id}_trial{ti.try_number}"


# Remove characters `+` and `:` from the run id
def _sanitize_run_id(run_id: str) -> str:
    return re.sub(r'[+:]', '', run_id)
