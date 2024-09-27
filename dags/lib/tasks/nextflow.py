from typing import List

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.context import Context

from lib.config import K8sContext, clin_datalake_bucket, s3_conn_id
from lib.operators.nextflow import NextflowOperator
from lib.operators.spark_etl import SparkETLOperator
from lib.utils_etl import ClinAnalysis


def prepare_svclustering_parental_origin(batch_ids: List[str], spark_jar: str, skip: str = ''):
    return SparkETLOperator.partial(
        task_id='prepare_svclustering_parental_origin',
        name='prepare-svclustering-parental-origin',
        steps='default',
        app_name=f'prepare_svclustering_parental_origin',
        spark_class='bio.ferlab.clin.etl.nextflow.PrepareSVClusteringParentalOrigin',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        skip=skip,
        target_batch_types=[ClinAnalysis.GERMLINE],  # Only run for germline batches
    ).expand(batch_id=batch_ids)


def svclustering_parental_origin(batch_ids: List[str], skip: str = ''):
    class SVClusteringParentalOrigin(NextflowOperator):
        template_fields = NextflowOperator.template_fields + ('batch_id', 'input_key', 'output_key')

        def __init__(self,
                     batch_id: str,
                     skip: bool = False,
                     **kwargs) -> None:
            super().__init__(
                k8s_context=K8sContext.ETL,
                skip=skip,
                **kwargs
            )

            self.batch_id = batch_id
            self.input_key = f's3a://{clin_datalake_bucket}/nextflow/svclustering_parental_origin_input/{batch_id}/{batch_id}.csv'
            self.output_key = f's3a://{clin_datalake_bucket}/nextflow/svclustering_parental_origin_output/{batch_id}'
            self.arguments = [
                'nextflow', 'run', 'ferlab/svclusteringpo',
                '-profile', '<docker/singularity/.../institute>',
                '-c', '/root/nextflow/config/nextflow.config',
                '--input', self.input_key,
                '--outdir', self.output_key,
                '--fasta', 's3a://cqgc-qa-app-datalake/public/refgenomes/hg38/Homo_sapiens_assembly38.fasta',
                '--fasta-fai', 's3a://cqgc-qa-app-datalake/public/refgenomes/hg38/Homo_sapiens_assembly38.fasta.fai',
                '--fasta-dict', 's3a://cqgc-qa-app-datalake/public/refgenomes/hg38/Homo_sapiens_assembly38.dict'
            ]

        def execute(self, context: Context, **kwargs):
            batch_type = context['ti'].xcom_pull(task_ids='detect_batch_type', key=self.batch_id)[0]
            if batch_type != ClinAnalysis.GERMLINE.value:
                raise AirflowSkipException(
                    f'Batch id \'{self.batch_id}\' of batch type \'{batch_type}\' is not germline')

            s3 = S3Hook(s3_conn_id)
            if not s3.check_for_key(self.input_key):
                raise AirflowSkipException(f'No CSV input file for batch id \'{self.batch_id}\'')

            super().execute(context, **kwargs)

    return SVClusteringParentalOrigin.partial(
        task_id='svclustering_parental_origin',
        name='svclustering-parental-origin',
        skip=skip
    ).expand(batch_id=batch_ids)
