from airflow.models import Variable

from lib.config import k8s_in_cluster, k8s_cluster_context, K8sContext, env, clin_scratch_bucket, clin_datalake_bucket, svclustering_batch_size
from lib.operators.base_kubernetes import KubeConfig
from lib.operators.nextflow import NextflowOperatorConfig

# This is meant to be used in DAGS depending on the KubernetesPodOperator.
# It may not be used in older dags, but please use it in new ones.
kube_config_etl = KubeConfig(
    in_cluster=k8s_in_cluster(K8sContext.ETL),
    cluster_context=k8s_cluster_context(K8sContext.ETL),
    namespace=Variable.get('kubernetes_namespace'),
    image_pull_secrets_name='images-registry-credentials'
)

# This is meant to be used in DAGS depending on the NextflowOperator.
nextflow_base_config = NextflowOperatorConfig(
    kube_config=kube_config_etl,
    on_finish_action='delete_pod',
    image='nextflow/nextflow:23.10.1',
    service_account_name='nextflow',
    minio_credentials_secret_name=f'cqgc-{env}-minio-app-nextflow',
    minio_credentials_secret_access_key='access_key',
    minio_credentials_secret_secret_key='secret_key',
    persistent_volume_claim_name=f'cqgc-{env}-nextflow-pvc',
    persistent_volume_sub_path='workspace',
    persistent_volume_mount_path="/mnt/workspace",
    nextflow_working_dir=f's3://{clin_scratch_bucket}/nextflow/scratch',
)

nextflow_svclustering_base_config = nextflow_base_config \
    .append_args(
        '--fasta', f's3://{clin_datalake_bucket}/public/refgenomes/hg38/Homo_sapiens_assembly38.fasta',
        '--fasta_fai', f's3://{clin_datalake_bucket}/public/refgenomes/hg38/Homo_sapiens_assembly38.fasta.fai',
        '--fasta_dict', f's3://{clin_datalake_bucket}/public/refgenomes/hg38/Homo_sapiens_assembly38.dict',
        '--filter_batch_size', f'{svclustering_batch_size}'
    )
