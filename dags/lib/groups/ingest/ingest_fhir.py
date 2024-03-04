from airflow.decorators import task_group

from lib.config import (K8sContext,
                        config_file, env)
from lib.operators.pipeline import PipelineOperator
from lib.operators.spark import SparkOperator
from lib.utils_etl import skip


@task_group(group_id='fhir')
def ingest_fhir(
        batch_id: str,
        color: str,
        skip_all: str,
        skip_import: str,
        skip_batch: str,
        spark_jar: str,
        import_main_class: str = 'bio.ferlab.clin.etl.FileImport'
):
    fhir_import = PipelineOperator(
        task_id='fhir_import',
        name='etl-ingest-fhir-import',
        k8s_context=K8sContext.DEFAULT,
        aws_bucket=f'cqgc-{env}-app-files-import',
        color=color,
        skip=skip(skip_all, skip_import),
        arguments=[
            import_main_class, batch_id, 'false', 'true',
        ],
    )

    fhir_export = PipelineOperator(
        task_id='fhir_export',
        name='etl-ingest-fhir-export',
        k8s_context=K8sContext.DEFAULT,
        aws_bucket=f'cqgc-{env}-app-datalake',
        color=color,
        skip=skip(skip_all, skip_batch),
        arguments=[
            'bio.ferlab.clin.etl.FhirExport', 'all',
        ],
    )

    fhir_normalize = SparkOperator(
        task_id='fhir_normalize',
        name='etl-ingest-fhir-normalize',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.fhir.FhirRawToNormalized',
        spark_config='config-etl-large',
        skip=skip(skip_all, skip_batch),
        spark_jar=spark_jar,
        arguments=[
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_ingest_fhir_normalize',
            '--destination', 'all'
        ],
    )

    fhir_import >> fhir_export >> fhir_normalize
