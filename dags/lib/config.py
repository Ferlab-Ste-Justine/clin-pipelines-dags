from enum import Enum

import kubernetes
from airflow.exceptions import AirflowConfigException
from airflow.models import Variable


class Env:
    TEST = 'test'  # Used for local development and testing
    QA = 'qa'
    STAGING = 'staging'
    PROD = 'prod'


class K8sContext:
    DEFAULT = 'default'
    ETL = 'etl'


# Hardcoded config names are currently tolerated in the code since the enum
# was introduced later. However, please use the enum for any new code.
class EtlConfig(Enum):
    SINGLETON = 'config-etl-singleton'
    SMALL = 'config-etl-small'
    MEDIUM = 'config-etl-medium'
    LARGE = 'config-etl-large'
    X_LARGE = 'config-etl-xlarge'

env = Variable.get('environment')
k8s_namespace = Variable.get('kubernetes_namespace')
k8s_context = {
    K8sContext.DEFAULT: Variable.get('kubernetes_context_default', None),
    K8sContext.ETL: Variable.get('kubernetes_context_etl', None),
}
base_url = Variable.get('base_url', None)
s3_conn_id = Variable.get('s3_conn_id', None)
s3_franklin = Variable.get('s3_franklin', None)
s3_franklin_bucket = Variable.get('s3_franklin_bucket', None)
s3_gnomad = Variable.get('s3_gnomad', None)
franklin_url = Variable.get('franklin_url', None)
franklin_email = Variable.get('franklin_email', None)
franklin_password = Variable.get('franklin_password', None)
slack_hook_url = Variable.get('slack_hook_url', None)
show_test_dags = Variable.get('show_test_dags', None) == 'yes'
cosmic_credentials = Variable.get('cosmic_credentials', None)
omim_credentials = Variable.get('omim_credentials', None)
basespace_illumina_credentials = Variable.get('basespace_illumina_credentials', None)
svclustering_batch_size = Variable.get('svclustering_batch_size', '100')
dev_skip_task = Variable.get('dev_skip_task', None) == 'yes'

clin_import_bucket = f'cqgc-{env}-app-files-import'
clin_datalake_bucket = f'cqgc-{env}-app-datalake'
clin_nextflow_bucket = f'cqgc-{env}-app-nextflow'
clin_scratch_bucket = f'cqgc-{env}-app-files-scratch'
all_qlin_buckets = [clin_import_bucket, clin_datalake_bucket, clin_nextflow_bucket, clin_scratch_bucket]

arranger_image = 'ferlabcrsj/clin-arranger:1.3.3'
aws_image = 'amazon/aws-cli'
curl_image = 'curlimages/curl'
fhir_csv_image = 'ferlabcrsj/csv-to-fhir'
postgres_image = 'ferlabcrsj/postgres-backup:9bb43092f76e95f17cd09f03a27c65d84112a3cd'
spark_image = 'ferlabcrsj/spark:65d1946780f97a8acdd958b89b64fad118c893ee'
spark_service_account = 'spark'
batch_ids = []
chromosomes = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', 'X', 'Y']

if env == Env.TEST:
    fhir_image = 'ferlabcrsj/clin-fhir'
    pipeline_image = 'ferlabcrsj/clin-pipelines'
    panels_image = 'ferlabcrsj/clin-panels:13b8182d493658f2c6e0583bc275ba26967667ab-1683653903'
    es_url = None
    spark_jar = None
    obo_parser_spark_jar = ''
    ca_certificates = None
    minio_certificate = None
    indexer_context = K8sContext.DEFAULT
    auth_url = None
    config_file = None
    franklin_assay_id = 'test_assay_id'
    batch_ids = []
elif env == Env.QA:
    fhir_image = 'ferlabcrsj/clin-fhir'
    pipeline_image = 'ferlabcrsj/clin-pipelines'
    panels_image = 'ferlabcrsj/clin-panels:13b8182d493658f2c6e0583bc275ba26967667ab-1683653903'
    es_url = 'http://elasticsearch:9200'
    spark_jar = 'clin-variant-etl-v3.31.2.jar'
    obo_parser_spark_jar = 'obo-parser-v1.1.0.jar' # deploy from https://github.com/Ferlab-Ste-Justine/obo-parser/tree/clin-v1.x.0
    ca_certificates = 'ingress-ca-certificate'
    minio_certificate = 'minio-ca-certificate'
    indexer_context = K8sContext.DEFAULT
    auth_url = 'https://auth.qa.cqgc.hsj.rtss.qc.ca'
    config_file = 'config/qa.conf'
    franklin_assay_id = '2765500d-8728-4830-94b5-269c306dbe71'
    batch_ids = [ # keep only the relevant ones for QA tests
        '1_data_to_import_germinal',
        '2_data_to_import_germinal',
        '3_data_to_import_somatic',
        '4_data_to_import_somatic_normal',
        '4_data_to_import_somatic_normal',
        '5_data_to_import_etl_run',
    ]
elif env == Env.STAGING:
    fhir_image = 'ferlabcrsj/clin-fhir:49ccd0c'
    pipeline_image = 'ferlabcrsj/clin-pipelines:a713d14'
    panels_image = 'ferlabcrsj/clin-panels:13b8182d493658f2c6e0583bc275ba26967667ab-1683653903'
    es_url = 'http://elasticsearch:9200'
    spark_jar = 'clin-variant-etl-v3.31.1.jar'
    obo_parser_spark_jar = 'obo-parser-v1.1.0.jar'
    ca_certificates = 'ingress-ca-certificate'
    minio_certificate = 'minio-ca-certificate'
    indexer_context = K8sContext.DEFAULT
    auth_url = 'https://auth.staging.cqgc.hsj.rtss.qc.ca'
    config_file = 'config/staging.conf'
    franklin_assay_id = '2765500d-8728-4830-94b5-269c306dbe71'
    batch_ids = [] # automatic dags/lib/tasks/batch_type:get_all_batch_ids
elif env == Env.PROD:
    fhir_image = 'ferlabcrsj/clin-fhir:49ccd0c'
    pipeline_image = 'ferlabcrsj/clin-pipelines:a713d14'
    panels_image = 'ferlabcrsj/clin-panels:13b8182d493658f2c6e0583bc275ba26967667ab-1683653903'
    es_url = 'https://workers.search.cqgc.hsj.rtss.qc.ca:9200'
    spark_jar = 'clin-variant-etl-v3.31.1.jar'
    obo_parser_spark_jar = 'obo-parser-v1.1.0.jar'
    ca_certificates = 'ca-certificates-bundle'
    minio_certificate = 'ca-certificates-bundle'
    indexer_context = K8sContext.ETL
    auth_url = 'https://auth.cqgc.hsj.rtss.qc.ca'
    config_file = 'config/prod.conf'
    franklin_assay_id = 'b8a30771-5689-4189-8157-c6063ad738d1'
    batch_ids = [] # automatic dags/lib/tasks/batch_type:get_all_batch_ids
else:
    raise AirflowConfigException(f'Unexpected environment "{env}"')


def env_url(prefix: str = '') -> str:
    return f'{prefix}{env}' if env in [Env.QA, Env.STAGING] else ''


def k8s_in_cluster(context: str) -> bool:
    return not k8s_context[context]


def k8s_config_file(context: str) -> str:
    return None if not k8s_context[context] else '~/.kube/config'


def k8s_cluster_context(context: str) -> str:
    return k8s_context[context]


def k8s_load_config(context: str) -> None:
    if not k8s_context[context]:
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.config.load_kube_config(
            config_file=k8s_config_file(context),
            context=k8s_context[context],
        )
