from airflow.decorators import task, task_group

from lib.config import K8sContext, env
from lib.operators.pipeline import PipelineOperator
from lib.tasks import params_validate
from lib.utils_etl import color


@task_group(group_id='convert_vcf_header')
def convert_vcf_header(analysis_ids: list, skip: str):
    """
    Convert VCF header from version 4.4 to 4.3 for somatic CNV files.
    """
    
    analysis_ids_arg = params_validate.prepare_analysis_ids_arg \
        .override(task_id='prepare_analysis_ids')(analysis_ids, skip=skip)
    
    convert_task = PipelineOperator(
        task_id='run_convert_vcf_header',
        name='convert-vcf-header',
        k8s_context=K8sContext.DEFAULT,
        aws_bucket=f'cqgc-{env}-app-files-import',
        color=color(),
        arguments=[
            'bio.ferlab.clin.etl.ConvertVCFHeader',
            analysis_ids_arg,
        ],
        skip=skip
    )
    
    analysis_ids_arg >> convert_task
