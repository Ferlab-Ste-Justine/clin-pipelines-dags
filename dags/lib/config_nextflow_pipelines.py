"""
This module contains configuration for nextflow pipelines
and their associated kubernetes config maps.

Centralizing the configuration of kubernetes config maps will
ease consistency in the nextflow long running pod that is used
for troubleshooting.  This pod is not managed by airflow
and is defined in another repository. All the nextflow config
maps must be mounted in this pod.

Centralization helps identify required config maps and their mount
paths. It also prevents mount path conflicts. Always use a unique
mount path for each config map.
"""
from lib.operators.base_kubernetes import ConfigMap
from lib.operators.nextflow import NextflowPipeline


####################################
# Define nextflow config maps here #
####################################


default_config_map = ConfigMap(
    name='nextflow',
    mount_path='/root/nextflow/config'
)
default_config_file = f"{default_config_map.mount_path}/nextflow.config"


##################################
# Define nextflow pipelines here #
##################################


base_nextflow_pipeline = NextflowPipeline(
    config_files=[default_config_file],
    config_maps=[default_config_map]
)

svclustering_pipeline = base_nextflow_pipeline \
    .with_url('Ferlab-Ste-Justine/ferlab-svclustering-parental-origin') \
    .with_revision('v1.1.1-clin')
