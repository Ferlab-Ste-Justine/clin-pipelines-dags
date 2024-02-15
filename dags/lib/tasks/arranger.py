from lib.config import K8sContext, env
from lib.operators.arranger import ArrangerOperator
from lib.operators.k8s_deployment_restart import K8sDeploymentRestartOperator


def remove_project():
    return ArrangerOperator(
        task_id='arranger_remove_project',
        name='etl-publish-arranger-remove-project',
        k8s_context=K8sContext.DEFAULT,
        cmds=[
            'node',
            '--experimental-modules=node',
            '--es-module-specifier-resolution=node',
            'cmd/remove_project.js',
            env,
        ],
    )


def restart(**kwargs):
    return K8sDeploymentRestartOperator(
        task_id='arranger_restart',
        k8s_context=K8sContext.DEFAULT,
        deployment='arranger',
        **kwargs
    )
