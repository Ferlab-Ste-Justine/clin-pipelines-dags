from lib.config import K8sContext
from lib.operators.pipeline import PipelineOperator


def notify(
        batch_id: str,
        color: str,
        skip: str
):
    return PipelineOperator(
        task_id='notify',
        name='etl-notify',
        k8s_context=K8sContext.DEFAULT,
        color=color,
        skip=skip,
        arguments=[
            'bio.ferlab.clin.etl.LDMNotifier', batch_id,
        ],
    )
