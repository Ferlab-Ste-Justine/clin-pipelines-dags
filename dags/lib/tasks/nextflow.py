from typing import List

from airflow.exceptions import AirflowSkipException
from airflow.models import MappedOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.context import Context

from lib.config import (clin_datalake_bucket, s3_conn_id, nextflow_base_config)
from lib.operators.nextflow import NextflowOperator
from lib.operators.spark_etl import SparkETLOperator
from lib.utils_etl import ClinAnalysis


def prepare_svclustering_parental_origin(
        batch_ids: List[str],
        spark_jar: str,
        skip: str = '') -> MappedOperator:
    return SparkETLOperator.partial(
        task_id='prepare_svclustering_parental_origin',
        name='prepare-svclustering-parental-origin',
        steps='default',
        app_name='prepare_svclustering_parental_origin',
        spark_class='bio.ferlab.clin.etl.nextflow.PrepareSVClusteringParentalOrigin',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        skip=skip,
        target_batch_types=[ClinAnalysis.GERMLINE],  # Only run for germline batches
    ).expand(batch_id=batch_ids)


def svclustering_parental_origin(batch_ids: List[str], skip: str = ''):
    class SVClusteringParentalOrigin(NextflowOperator):
        template_fields = [
            *NextflowOperator.template_fields,
            'batch_id',
            'input_key',
            'output_key'
        ]

        def __init__(self,
                     batch_id: str,
                     **kwargs) -> None:
            super().__init__(
                **kwargs
            )

            self.batch_id = batch_id
            self.input_key = f's3://{clin_datalake_bucket}/nextflow/svclustering_parental_origin_input/{batch_id}/{batch_id}.csv'
            self.output_key = f's3://{clin_datalake_bucket}/nextflow/svclustering_parental_origin_output/{batch_id}'
            self.arguments = [
                *self.arguments,
                '--input', self.input_key,
                '--outdir', self.output_key
            ]

        def execute(self, context: Context):
            batch_type = context['ti'].xcom_pull(
                task_ids='detect_batch_type',
                key=self.batch_id
            )[0]
            if batch_type != ClinAnalysis.GERMLINE.value:
                raise AirflowSkipException(
                    f'Batch id \'{self.batch_id}\' of batch type \'{batch_type}\' is not germline')

            s3 = S3Hook(s3_conn_id)
            if not s3.check_for_key(self.input_key):
                raise AirflowSkipException(f'No CSV input file for batch id \'{self.batch_id}\'')

            super().execute(context)

    return nextflow_base_config \
        .with_pipeline('Ferlab-Ste-Justine/ferlab-svclustering-parental-origin') \
        .with_revision('v1.1.1-clin') \
        .append_args(
            '--fasta', f's3://{clin_datalake_bucket}/public/refgenomes/hg38/Homo_sapiens_assembly38.fasta',
            '--fasta_fai', f's3://{clin_datalake_bucket}/public/refgenomes/hg38/Homo_sapiens_assembly38.fasta.fai',
            '--fasta_dict', f's3://{clin_datalake_bucket}/public/refgenomes/hg38/Homo_sapiens_assembly38.dict') \
        .partial(
            SVClusteringParentalOrigin,
            task_id='svclustering_parental_origin',
            name='svclustering_parental_origin',
            skip=skip
        ).expand(batch_id=batch_ids)
