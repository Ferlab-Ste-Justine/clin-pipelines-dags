from airflow.decorators import task, task_group

from lib.config import K8sContext
from lib.operators.pipeline import PipelineOperator
from lib.utils_etl import color


@task_group(group_id='convert_vcf_header')
def convert_vcf_header(analysis_ids: list, skip: str):
    """
    Convert VCF header from version 4.4 to 4.3 for somatic CNV files.
    """
    
    @task(task_id='prepare_analysis_ids')
    def prepare_analysis_ids(analysis_ids: list) -> str:
        """Convert analysis_ids list to comma-separated string argument"""
        if not analysis_ids or len(analysis_ids) == 0:
            return '--analysis-ids='
        return '--analysis-ids=' + ','.join(analysis_ids)
    
    analysis_ids_arg = prepare_analysis_ids(analysis_ids)
    
    convert_task = PipelineOperator(
        task_id='run_convert_vcf_header',
        name='convert-vcf-header',
        k8s_context=K8sContext.DEFAULT,
        color=color(),
        arguments=[
            'bio.ferlab.clin.etl.ConvertVCFHeader',
            analysis_ids_arg,
        ],
        skip=skip
    )
    
    analysis_ids_arg >> convert_task
