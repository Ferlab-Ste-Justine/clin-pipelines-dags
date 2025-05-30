from airflow.exceptions import AirflowSkipException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from lib import config
from lib.config import auth_url, env_url
from lib.utils import join


class FhirOperator(KubernetesPodOperator):

    template_fields = KubernetesPodOperator.template_fields + (
        'color','skip'
    )

    def __init__(
        self,
        k8s_context: str,
        color: str = '',
        skip: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            on_finish_action='delete_pod',
            in_cluster=config.k8s_in_cluster(k8s_context),
            config_file=config.k8s_config_file(k8s_context),
            cluster_context=config.k8s_cluster_context(k8s_context),
            namespace=config.k8s_namespace,
            image=config.fhir_image,
            **kwargs,
        )
        self.skip = skip
        self.color = color

    def execute(self, **kwargs):

        if self.skip:
            raise AirflowSkipException()

        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(
                name='images-registry-credentials',
            ),
        ]
        self.env_vars = [
            k8s.V1EnvVar(
                name='BASE_URL',
                value='https://' + join('-', ['fhir', self.color]) + env_url('.') +
                '.cqgc.hsj.rtss.qc.ca/fhir',
            ),
            k8s.V1EnvVar(
                name='OAUTH_URL',
                value=auth_url + '/realms/clin/protocol/openid-connect/token',
            ),
            k8s.V1EnvVar(
                name='OAUTH_CLIENT_ID',
                value='clin-system',
            ),
            k8s.V1EnvVar(
                name='OAUTH_CLIENT_SECRET',
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name='keycloak-client-system-credentials',
                        key='client-secret',
                    ),
                ),
            ),
            k8s.V1EnvVar(
                name='OAUTH_UMA_AUDIENCE',
                value='clin-acl',
            ),
        ]

        super().execute(**kwargs)
