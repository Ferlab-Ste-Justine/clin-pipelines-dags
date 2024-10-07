"""
This module contains configuration for the nextflow operator and its associated kubernetes config maps.

Centralizing the configuration of kubernetes config maps will ease consistency in the nextflow long-running pod that is
used for troubleshooting.  This pod is not managed by Airflow and is defined in another repository. All the nextflow
config maps must be mounted in this pod.

Centralization helps identify required config maps and their mount paths. It also prevents mount path conflicts. Always
use a unique mount path for each config map.
"""
from lib.operators.base_kubernetes import ConfigMap


####################################
# Define nextflow config maps here #
####################################


default_nextflow_config_map = ConfigMap(
    name='nextflow',
    mount_path='/root/nextflow/config'
)
default_nextflow_config_file = f"{default_nextflow_config_map.mount_path}/nextflow.config"

variant_annotation_config_map = ConfigMap(
    name='nextflow-variant-annotation',
    mount_path='/root/nextflow/variant_annotation'
)
variant_annotation_config_file = f"{variant_annotation_config_map.mount_path}/variant_annotation_v2.x.config"
variant_annotation_params_file = f"{variant_annotation_config_map.mount_path}/params_v2.x.json"