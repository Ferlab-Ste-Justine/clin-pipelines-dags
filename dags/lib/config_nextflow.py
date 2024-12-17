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

##################################
# Define nextflow revisions here #
##################################
nextflow_svclustering_revision = 'v1.2.0-clin'
nextflow_svclustering_parental_origin_revision = 'v1.1.1-clin'

#######################################
# Define nextflow pipeline names here #
#######################################
nextflow_svclustering_pipeline = 'Ferlab-Ste-Justine/ferlab-svclustering'
nextflow_svclustering_parental_origin_pipeline = 'Ferlab-Ste-Justine/ferlab-svclustering-parental-origin'
