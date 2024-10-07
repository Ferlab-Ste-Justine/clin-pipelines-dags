import copy
from dataclasses import dataclass, field, fields
from typing import Optional, List, Type, TypeVar
from typing_extensions import Self

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.context import Context
from kubernetes.client import models as k8s


# This modules contains utility classes and functions to simplify
# configuration and usage of the KubernetesPodOperator.
# For consistency, we recommend leveraging these for new operators.

T = TypeVar("T")


def required() -> T:
    """
    Use this to initialize a required field in a dataclass.

    When inheriting from a dataclass with non-required fields, it is
    not allowed by default to make a field required. You would
    encounter an error like this:
    `TypeError: non-default argument follows default argument`.

    This function is a workaround to avoid this problem.

    Example usage:
    ```
        from dataclasses import dataclass
        from base_kubernetes import required

        @dataclass
        class Base:
            optional_field: Optional[str] = None

        @dataclass
        class Foo(Base):
            required_field: str = required()
    ```
    """
    f: T

    def factory() -> T:
        # mypy treats a Field as a T, even though it has attributes like .name, .default, etc
        field_name = f.name  # type: ignore[attr-defined]
        raise ValueError(f"field '{field_name}' required")

    f = field(default_factory=factory)
    return f


@dataclass
class KubeConfig:
    """
    KubeConfig is a dataclass that contains the configuration for a BaseKubernetesOperator
    :param in_cluster: bool - @see KubernetesPodOperator.in_cluster
    :param cluster_context: Optional[str] - @see KubernetesPodOperator.cluster_context
    :param namespace: Optional[str] - @see KubernetesPodOperator.namespace
    :param image_pull_secrets_name: Optional[str] - @see KubernetesPodOperator.image_pull_secrets
    """
    in_cluster: bool = True,
    cluster_context: Optional[str] = None
    namespace: Optional[str] = None
    image_pull_secrets_name: Optional[str] = None


class BaseKubernetesOperator(KubernetesPodOperator):
    template_fields = [
        *KubernetesPodOperator.template_fields,
        'image_pull_secrets_name'
    ]

    def __init__(
            self,
            image_pull_secrets_name: Optional[str] = None,
            **kwargs
    ) -> None:
        super().__init__(
            **kwargs
        )
        self.image_pull_secrets_name = image_pull_secrets_name

    def execute(self, context: Context, **kwargs):
        if self.image_pull_secrets_name:
            self.image_pull_secrets = [
                k8s.V1LocalObjectReference(
                    name=self.image_pull_secrets_name,
                ),
            ]
        super().execute(context, **kwargs)


@dataclass
class BaseConfig:
    """
    BaseConfig is a dataclass that contains the configuration for a BaseKubernetesOperator
    :param kube_config: KubeConfig
    :param is_delete_operator_pod: bool
    :param image: Optional[str]
    :param service_account_name: Optional[str] - @see KubernetesPodOperator.service_account_name
    """
    kube_config: KubeConfig
    is_delete_operator_pod: bool = False
    image: Optional[str] = None
    arguments: List[str] = field(default_factory=list)
    service_account_name: Optional[str] = None

    def append_args(self, *new_args) -> Self:
        c = copy.copy(self)
        c.arguments = [*self.arguments, *new_args]
        return c

    def prepend_args(self, *new_args) -> Self:
        c = copy.copy(self)
        c.arguments = [*new_args, *self.arguments]
        return c

    def with_image(self, new_image) -> Self:
        c = copy.copy(self)
        c.image = new_image
        return c

    def build_operator(self,
                       class_to_instantiate: Type[BaseKubernetesOperator],
                       **kwargs) -> BaseKubernetesOperator:
        # Using a shallow copy method (recommended in dataclasses.asdict)
        # to avoid converting complex fields to dictionaries.
        this_params = {
            field.name: getattr(self, field.name) for field in fields(self)
        }
        this_params.pop('kube_config', None)
        params = {**this_params, **kwargs}
        return class_to_instantiate(
            in_cluster=self.kube_config.in_cluster,
            cluster_context=self.kube_config.cluster_context,
            namespace=self.kube_config.namespace,
            **params
        )

    def partial(self,
                class_to_instantiate: Type[BaseKubernetesOperator] = BaseKubernetesOperator,
                **kwargs):
        # Using a shallow copy method (recommended in dataclasses.asdict)
        # to avoid converting complex fields to dictionaries.
        this_params = {
            field.name: getattr(self, field.name) for field in fields(self)
        }
        this_params.pop('kube_config', None)
        params = {**this_params, **kwargs}
        return class_to_instantiate.partial(
            in_cluster=self.kube_config.in_cluster,
            cluster_context=self.kube_config.cluster_context,
            namespace=self.kube_config.namespace,
            **params
        )


@dataclass
class ConfigMap:
    """
    Represents a Kubernetes ConfigMap that contains files to be
    mounted in a nextflow pod.
    """
    name: str
    mount_path: str
