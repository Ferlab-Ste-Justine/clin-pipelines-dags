from dataclasses import dataclass
from types import SimpleNamespace
from typing import List


@dataclass
class NextflowConfigMap:
    """
    Represents a Kubernetes ConfigMap that contains files to be
    mounted in a nextflow pod.
    """
    name: str
    mount_path: str


@dataclass
class NextflowPipeline:
    """
    Represents a nextflow pipeline to be executed in a nextflow pod.
    """
    name: str
    url: str
    revision: str
    config_maps: List[NextflowConfigMap]
    config_files: List[str]
    params_file: str
    meta: SimpleNamespace = None


###########################
# Define config maps here #
###########################


default_config_map = NextflowConfigMap(
    name='nextflow',
    mount_path='/root/nextflow/config'
)
default_config_file = f"{default_config_map.mount_path}/nextflow.config"

variant_annotation_config_map = NextflowConfigMap(
    name='nextflow-variant-annotation',
    mount_path='/root/nextflow/variant_annotation',
)


##################################
# Define nextflow pipelines here #
##################################


# TODO: adjust revision number and file names
variant_annotation_pipeline = NextflowPipeline(
    name='variant_annotation',
    url='Ferlab-Ste-Justine/Post-processing-Pipeline',
    revision='c1fc77177c7113f0a53f8409ac0fbab7c24c4751',
    config_maps=[default_config_map, variant_annotation_config_map],
    params_file=f"{variant_annotation_config_map.mount_path}/params_v2.x.json",
    config_files=[
        default_config_file,
        f"{variant_annotation_config_map.mount_path}/variant_annotation_v2.x.config"
    ]
)
