from lib.config import K8sContext
from lib.operators.notify import NotifyOperator


def notify(
        batch_id: str,
        color: str,
        skip: str
):
    return NotifyOperator(
        task_id='notify',
        name='etl-notify',
        k8s_context=K8sContext.DEFAULT,
        batch_id=batch_id,
        color=color,
        skip=skip
    )
