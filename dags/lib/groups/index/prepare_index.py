from airflow.decorators import task_group

from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator


@task_group(group_id='prepare')
def prepare_index(
        release_id: str,
        spark_jar: str,
):
    gene_centric = SparkOperator(
        task_id='gene_centric',
        name='etl-prepare-gene-centric',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        arguments=[
            'gene_centric',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_gene_centric',
            '--releaseId', release_id
        ],
    )

    gene_suggestions = SparkOperator(
        task_id='gene_suggestions',
        name='etl-prepare-gene-suggestions',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        arguments=[
            'gene_suggestions',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_gene_suggestions',
            '--releaseId', release_id
        ],
    )

    variant_centric = SparkOperator(
        task_id='variant_centric',
        name='etl-prepare-variant-centric',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        arguments=[
            'variant_centric',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_variant_centric',
            '--releaseId', release_id
        ],
    )

    variant_suggestions = SparkOperator(
        task_id='variant_suggestions',
        name='etl-prepare-variant-suggestions',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        arguments=[
            'variant_suggestions',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_variant_suggestions',
            '--releaseId', release_id
        ],
    )

    cnv_centric = SparkOperator(
        task_id='cnv_centric',
        name='etl-prepare-cnv-centric',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        arguments=[
            'cnv_centric',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_cnv_centric',
            '--releaseId', release_id
        ],
    )

    coverage_by_gene_centric = SparkOperator(
        task_id='coverage_by_gene',
        name='etl-prepare-coverage-by-gene',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.es.PrepareIndex',
        spark_config='config-etl-large',
        spark_jar=spark_jar,
        arguments=[
            'coverage_by_gene_centric',
            '--config', config_file,
            '--steps', 'initial',
            '--app-name', 'etl_prepare_coverage_by_gene',
            '--releaseId', release_id
        ],
    )

    gene_centric >> gene_suggestions >> variant_centric >> variant_suggestions >> cnv_centric >> coverage_by_gene_centric
