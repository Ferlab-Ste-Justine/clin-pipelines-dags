from typing import List

from airflow.decorators import task_group
from lib.config import K8sContext, config_file, env
from lib.operators.pipeline import PipelineOperator
from lib.operators.pipeline_fhir_import import PipelineFhirImportOperator
from lib.operators.spark import SparkOperator
from lib.tasks.params import prepare_expand_batch_ids
from lib.utils_etl import skip


@task_group(group_id='fhir')
def ingest_fhir(
        batch_ids: List[str],
        color: str,
        skip_all: str,
        skip_import: str,
        skip_post_import: str,
        spark_jar: str,
        import_main_class: str = 'bio.ferlab.clin.etl.FileImport'
):

    prepare_expand_batch_ids_task = prepare_expand_batch_ids(batch_ids, skip(skip_all, skip_import))

    fhir_import = PipelineFhirImportOperator.partial(
        task_id='fhir_import',
        color=color,
        main_class=import_main_class,
        skip=skip(skip_all, skip_import),
        dry_run=False,
        full_metadata=True,
    ).expand(batch_id=prepare_expand_batch_ids_task)

    fhir_export = PipelineOperator(
        task_id='fhir_export',
        name='etl-ingest-fhir-export',
        k8s_context=K8sContext.DEFAULT,
        aws_bucket=f'cqgc-{env}-app-datalake',
        color=color,
        skip=skip(skip_all, skip_post_import),
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
        skip=skip(skip_all, skip_post_import),
        spark_jar=spark_jar,
        arguments=[
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_ingest_fhir_normalize',
            '--destination', 'all'
        ],
    )

    fhir_enrich_clinical = SparkOperator(
        task_id='fhir_enrich_clinical',
        name='etl-ingest-fhir-enrich-clinical',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.fhir.EnrichedClinical',
        spark_config='config-etl-large',
        skip=skip(skip_all, skip_post_import),
        spark_jar=spark_jar,
        arguments=[
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_ingest_fhir_enrich_clinical',
        ],
    )

    prepare_expand_batch_ids_task >> fhir_import >> fhir_export >> fhir_normalize >> fhir_enrich_clinical
