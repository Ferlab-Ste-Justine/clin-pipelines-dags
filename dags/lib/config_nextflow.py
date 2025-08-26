"""
This module contains configuration for the nextflow operator and its associated kubernetes config maps.

Centralizing the configuration of kubernetes config maps will ease consistency in the nextflow long-running pod that is
used for troubleshooting.  This pod is not managed by Airflow and is defined in another repository. All the nextflow
config maps must be mounted in this pod.

Centralization helps identify required config maps and their mount paths. It also prevents mount path conflicts. Always
use a unique mount path for each config map.
"""
from lib import config
from lib.operators.base_kubernetes import ConfigMap

NEXTFLOW_MAIN_CLASS = 'bio.ferlab.clin.etl.nextflow.RunNextflow'

####################################
# Define nextflow config maps here #
####################################
default_nextflow_config_map = ConfigMap(
    name='nextflow',
    mount_path='/root/nextflow/config'
)
default_nextflow_config_file = f"{default_nextflow_config_map.mount_path}/nextflow.config"

nextflow_post_processing_config_map = ConfigMap(
    name='nextflow-postprocessing',
    mount_path='/root/nextflow/config/postprocessing'
)
nextflow_post_processing_config_file = f"{nextflow_post_processing_config_map.mount_path}/postprocessing.config"
nextflow_post_processing_params_file = f"{nextflow_post_processing_config_map.mount_path}/postprocessing-params.json"

nextflow_cnv_post_processing_config_map = ConfigMap(
    name='nextflow-cnv-postprocessing',
    mount_path='/root/nextflow/config/cnv-postprocessing'
)
nextflow_cnv_post_processing_config_file = f"{nextflow_cnv_post_processing_config_map.mount_path}/cnv-postprocessing.config"
nextflow_cnv_post_processing_params_file = f"{nextflow_cnv_post_processing_config_map.mount_path}/cnv-postprocessing-params.json"


##################################
# Define nextflow revisions here #
##################################
nextflow_svclustering_revision = 'v1.3.2-clin'
nextflow_svclustering_parental_origin_revision = 'v1.1.1-clin'
nextflow_post_processing_revision = 'v2.8.0'
nextflow_cnv_post_processing_revision = 'v1.0.0'

#######################################
# Define nextflow pipeline names here #
#######################################
nextflow_svclustering_pipeline = 'Ferlab-Ste-Justine/ferlab-svclustering'
nextflow_svclustering_parental_origin_pipeline = 'Ferlab-Ste-Justine/ferlab-svclustering-parental-origin'
nextflow_post_processing_pipeline = 'Ferlab-Ste-Justine/Post-processing-Pipeline'
nextflow_cnv_post_processing_pipeline = 'Ferlab-Ste-Justine/cnv-post-processing'


#############################################################
# Define nextflow input and output file configurations here #
#############################################################
nextflow_bucket = config.clin_nextflow_bucket
nextflow_exomiser_input_key = lambda analysis_id: f'exomiser/input/{analysis_id}.pheno.json'

nextflow_svclustering_germline_input_key = 'svclustering/input/germline.samplesheet.csv'
nextflow_svclustering_somatic_input_key = 'svclustering/input/somatic.samplesheet.csv'
nextflow_svclustering_germline_output_key = 'svclustering/output/germline'
nextflow_svclustering_somatic_output_key = 'svclustering/output/somatic'

nextflow_svclustering_parental_origin_input_key = lambda batch_id: \
    f'svclustering_parental_origin/input/{batch_id}/{batch_id}.csv'
nextflow_svclustering_parental_origin_output_key = lambda batch_id: \
    f'svclustering_parental_origin/output/{batch_id}'

nextflow_post_processing_input_key = lambda _hash: f'post_processing/input/{_hash}.samplesheet.csv'
nextflow_post_processing_info_output_key = lambda _hash: f'post_processing/output/runs/{_hash}'  # _hash here can be a template
nextflow_post_processing_vep_output_key = 'post_processing/output/ensemblvep'
nextflow_post_processing_exomiser_output_key = 'post_processing/output/exomiser'

nextflow_cnv_post_processing_input_key = lambda _hash: f'cnv_post_processing/input/{_hash}.samplesheet.csv'
nextflow_cnv_post_processing_info_output_key = lambda _hash: f'cnv_post_processing/output/runs/{_hash}'  # _hash here can be a template
nextflow_cnv_post_processing_exomiser_output_key = 'cnv_post_processing/output/exomiser'
