from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
from kubernetes.client import models as k8s


def add_working_dir_to_pod_spec(working_dir, existing_pod_spec=None, container_name=None):
    """
    Adds a working directory to the pod specification.

    Parameters:
    working_dir (str): The working directory to set in the pod.
    container_name (str): The name of the container to set the working directory for.
    existing_pod_spec (k8s.V1PodSpec, optional): An existing pod specification to reconcile with.

    Returns:
    k8s.V1PodSpec: The reconciled pod specification.
    """
    if container_name is None:
        if existing_pod_spec and existing_pod_spec.containers:
            container_name = existing_pod_spec.containers[0].name
        else:
            container_name = KubernetesPodOperator.BASE_CONTAINER_NAME
    pod_spec = k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name=container_name,
                    working_dir=working_dir
                )
            ]
        )
    )

    # Reconcile the new pod spec with the existing one, if provided
    return PodGenerator.reconcile_pods(pod_spec, existing_pod_spec)

