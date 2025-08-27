from lib.config import Env, K8sContext, env_url
from lib.doc import qa as doc
from lib.operators.spark import SparkOperator


def non_empty_tables(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='non_empty_tables',
        doc_md=doc.non_empty_tables,
        name='etl-qc-non-empty-tables',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.tables.NonEmptyTables',
        spark_config='config-etl-medium',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_gnomad_joint(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_gnomad_joint',
        doc_md=doc.no_dup_gnomad_joint,
        name='etl-qc-no-dup-gnomad-joint',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationGnomadJoint',
        spark_config='config-etl-medium',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_clinical(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_clinical',
        doc_md=doc.no_dup_clinical,
        name='etl-qc-no-dup-clinical',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationClinical',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_nor_snv(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_nor_snv',
        doc_md=doc.no_dup_nor_snv,
        name='etl-qc-no-dup-nor-snv',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationNorSNV',
        spark_config='config-etl-medium',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_nor_snv_somatic(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_nor_snv_somatic',
        doc_md=doc.no_dup_nor_snv_somatic,
        name='etl-qc-no-dup-nor-snv-somatic',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationNorSNVSomatic',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_nor_consequences(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_nor_consequences',
        doc_md=doc.no_dup_nor_consequences,
        name='etl-qc-no-dup-nor-consequences',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationNorConsequences',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_nor_variants(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_nor_variants',
        doc_md=doc.no_dup_nor_variants,
        name='etl-qc-no-dup-nor-variants',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationNorVariants',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_snv(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_snv',
        doc_md=doc.no_dup_snv,
        name='etl-qc-no-dup-snv',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV',
        spark_config='config-etl-medium',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_snv_somatic(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_snv_somatic',
        doc_md=doc.no_dup_snv_somatic,
        name='etl-qc-no-dup-snv-somatic',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNVSomatic',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_consequences(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_consequences',
        doc_md=doc.no_dup_consequences,
        name='etl-qc-no-dup-consequences',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationConsequences',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_variants(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_variants',
        doc_md=doc.no_dup_variants,
        name='etl-qc-no-dup-variants',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationVariants',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_variant_centric(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_variant_centric',
        doc_md=doc.no_dup_variant_centric,
        name='etl-qc-no-dup-variant-centric',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationVariantCentric',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_cnv_centric(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_cnv_centric',
        doc_md=doc.no_dup_cnv_centric,
        name='etl-qc-no-dup-cnv-centric',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationCNV',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_coverage_by_gene(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_coverage_by_gene',
        doc_md=doc.no_dup_coverage_by_gene,
        name='etl-qc-no-dup-coverage-by-gene',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationCoverageByGene',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_nextflow_svclustering_germline(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_nextflow_svclusterin_germline',
        doc_md=doc.no_dup_nextflow_svclustering_germline,
        name='etl-qc-no-dup-nextflow-svclustering-germline',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationNextflowSVClusteringGermline',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_nextflow_svclustering_somatic(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_nextflow_svclustering_somatic',
        doc_md=doc.no_dup_nextflow_svclustering_somatic,
        name='etl-qc-no-dup-nextflow-svclustering-somatic',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationNextflowSVClusteringSomatic',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def no_dup_nextflow_svclustering_parental_origin(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='no_dup_nextflow_svclustering_parental_origin',
        doc_md=doc.no_dup_nextflow_svclustering_parental_origin,
        name='etl-qc-no-dup-nextflow-svclustering-parental-origin',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.NonDuplicationNextflowSVClusteringParentalOrigin',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def same_list_nor_snv_nor_variants(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='same_list_nor_snv_nor_variants',
        doc_md=doc.same_list_nor_snv_nor_variants,
        name='etl-qc-same-list-nor-snv-nor-variants',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenNorSNVAndNorVariants',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def same_list_nor_snv_somatic_nor_variants(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='same_list_nor_snv_somatic_nor_variants',
        doc_md=doc.same_list_nor_snv_somatic_nor_variants,
        name='etl-qc-same-list-nor-snv-somatic-nor-variants',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenNorSNVSomaticAndNorVariants',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def same_list_snv_variants(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='same_list_snv_variants',
        doc_md=doc.same_list_snv_variants,
        name='etl-qc-same-list-snv-variants',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenSNVAndVariants',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def same_list_snv_somatic_variants(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='same_list_snv_somatic_variants',
        doc_md=doc.same_list_snv_somatic_variants,
        name='etl-qc-same-list-snv-somatic-variants',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenSNVSomaticAndVariants',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )


def same_list_variants_variant_centric(spark_jar: str) -> SparkOperator:
    return SparkOperator(
        task_id='same_list_variants_variant_centric',
        doc_md=doc.same_list_variants_variant_centric,
        name='etl-qc-same-list-variants-variant-centric',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.clin.etl.qc.variantlist.SameListBetweenVariantsAndVariantCentric',
        spark_config='config-etl-small',
        spark_jar=spark_jar,
        arguments=['clin' + env_url('_')],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )
