from airflow.utils.task_group import TaskGroup
from lib.config import Env, K8sContext, env_url
from lib.doc import qc as doc
from lib.operators.spark import SparkOperator


def qc(
    group_id: str,
    spark_jar: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        vcf_snv = SparkOperator(
            task_id='vcf_snv',
            doc_md=doc.vcf_snv,
            name='etl-qc-vcf-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.fromVCF.ContainedInSNV',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        vcf_nor_variants = SparkOperator(
            task_id='vcf_nor_variants',
            doc_md=doc.vcf_nor_variants,
            name='etl-qc-vcf-nor-variants',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.fromVCF.ContainedInNorVariants',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        vcf_snv_somatic = SparkOperator(
            task_id='vcf_snv_somatic',
            doc_md=doc.vcf_snv_somatic,
            name='etl-qc-vcf-snv-somatic',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.fromVCF.ContainedInSNVSomatic',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        filters_snv = SparkOperator(
            task_id='filters_snv',
            doc_md=doc.filters_snv,
            name='etl-qc-filters-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantfilter.FiltersSNV',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        filters_frequency_extra = SparkOperator(
            task_id='filters_frequency_extra',
            doc_md=doc.filters_frequency_extra,
            name='etl-qc-filters-frequency-extra',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantfilter.FiltersFrequencyExtra',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        filters_frequency_missed = SparkOperator(
            task_id='filters_frequency_missed',
            doc_md=doc.filters_frequency_missed,
            name='etl-qc-filters-frequency-missed',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.variantfilter.FiltersFrequencyMissed',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_null_variant_centric = SparkOperator(
            task_id='no_null_variant_centric',
            doc_md=doc.no_null_variant_centric,
            name='etl-qc-no-null-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullVariantCentric',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_null_variant_centric_donors = SparkOperator(
            task_id='no_null_variant_centric_donors',
            doc_md=doc.no_null_variant_centric_donors,
            name='etl-qc-no-null-variant-centric-donors',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullVariantCentric_Donors',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_null_variant_centric_freqbyanal = SparkOperator(
            task_id='no_null_variant_centric_freqbyanal',
            doc_md=doc.no_null_variant_centric_freqbyanal,
            name='etl-qc-no-null-variant-centric-freqbyanal',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullVariantCentric_FreqByAnal',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_null_variant_centric_freqrqdm = SparkOperator(
            task_id='no_null_variant_centric_freqrqdm',
            doc_md=doc.no_null_variant_centric_freqrqdm,
            name='etl-qc-no-null-variant-centric-freqrqdm',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullVariantCentric_FreqRQDM',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_null_gene_centric = SparkOperator(
            task_id='no_null_gene_centric',
            doc_md=doc.no_null_gene_centric,
            name='etl-qc-no-null-gene-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullGene',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_null_cnv_centric = SparkOperator(
            task_id='no_null_cnv_centric',
            doc_md=doc.no_null_cnv_centric,
            name='etl-qc-no-null-cnv-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullCNV',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        no_null_coverage_by_gene = SparkOperator(
            task_id='no_null_coverage_by_gene',
            doc_md=doc.no_null_coverage_by_gene,
            name='etl-qc-no-null-coverage-by-gene',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainNoNullCoverageByGene',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric = SparkOperator(
            task_id='only_null_variant_centric',
            doc_md=doc.only_null_variant_centric,
            name='etl-qc-only-null-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric_cmc = SparkOperator(
            task_id='only_null_variant_centric_cmc',
            doc_md=doc.only_null_variant_centric_cmc,
            name='etl-qc-only-null-variant-centric-cmc',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric_CMC',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric_clinvar = SparkOperator(
            task_id='only_null_variant_centric_clinvar',
            doc_md=doc.only_null_variant_centric_clinvar,
            name='etl-qc-only-null-variant-centric-clinvar',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric_Clinvar',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric_consequences = SparkOperator(
            task_id='only_null_variant_centric_consequences',
            doc_md=doc.only_null_variant_centric_consequences,
            name='etl-qc-only-null-variant-centric-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric_Consequences',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric_donors = SparkOperator(
            task_id='only_null_variant_centric_donors',
            doc_md=doc.only_null_variant_centric_donors,
            name='etl-qc-only-null-variant-centric-donors',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric_Donors',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric_exomax = SparkOperator(
            task_id='only_null_variant_centric_exomax',
            doc_md=doc.only_null_variant_centric_exomax,
            name='etl-qc-only-null-variant-centric-exomax',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric_ExoMax',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric_extfreq = SparkOperator(
            task_id='only_null_variant_centric_extfreq',
            doc_md=doc.only_null_variant_centric_extfreq,
            name='etl-qc-only-null-variant-centric-extfreq',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric_ExtFreq',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric_framax = SparkOperator(
            task_id='only_null_variant_centric_framax',
            doc_md=doc.only_null_variant_centric_framax,
            name='etl-qc-only-null-variant-centric-framax',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric_FraMax',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric_freqbyanal = SparkOperator(
            task_id='only_null_variant_centric_freqbyanal',
            doc_md=doc.only_null_variant_centric_freqbyanal,
            name='etl-qc-only-null-variant-centric-freqbyanal',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric_FreqByAnal',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric_freqrqdm = SparkOperator(
            task_id='only_null_variant_centric_freqrqdm',
            doc_md=doc.only_null_variant_centric_freqrqdm,
            name='etl-qc-only-null-variant-centric-freqrqdm',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric_FreqRQDM',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_variant_centric_freqrqdmtumor = SparkOperator(
            task_id='only_null_variant_centric_freqrqdmtumor',
            doc_md=doc.only_null_variant_centric_freqrqdmtumor,
            name='etl-qc-only-null-variant-centric-freqrqdmtumor',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric_FreqRQDMTumor',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_gene_centric = SparkOperator(
            task_id='only_null_gene_centric',
            doc_md=doc.only_null_gene_centric,
            name='etl-qc-only-null-gene-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullGene',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_cnv_centric = SparkOperator(
            task_id='only_null_cnv_centric',
            doc_md=doc.only_null_cnv_centric,
            name='etl-qc-only-null-cnv-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullCNV',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        only_null_coverage_by_gene = SparkOperator(
            task_id='only_null_coverage_by_gene',
            doc_md=doc.only_null_coverage_by_gene,
            name='etl-qc-only-null-coverage-by-gene',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullCoverageByGene',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric = SparkOperator(
            task_id='same_value_variant_centric',
            doc_md=doc.same_value_variant_centric,
            name='etl-qc-same-value-variant-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric_cmc = SparkOperator(
            task_id='same_value_variant_centric_cmc',
            doc_md=doc.same_value_variant_centric_cmc,
            name='etl-qc-same-value-variant-centric-cmc',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric_CMC',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric_clinvar = SparkOperator(
            task_id='same_value_variant_centric_clinvar',
            doc_md=doc.same_value_variant_centric_clinvar,
            name='etl-qc-same-value-variant-centric-clinvar',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric_Clinvar',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric_consequences = SparkOperator(
            task_id='same_value_variant_centric_consequences',
            doc_md=doc.same_value_variant_centric_consequences,
            name='etl-qc-same-value-variant-centric-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric_Consequences',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric_donors = SparkOperator(
            task_id='same_value_variant_centric_donors',
            doc_md=doc.same_value_variant_centric_donors,
            name='etl-qc-same-value-variant-centric-donors',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric_Donors',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric_exomax = SparkOperator(
            task_id='same_value_variant_centric_exomax',
            doc_md=doc.same_value_variant_centric_exomax,
            name='etl-qc-same-value-variant-centric-exomax',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric_ExoMax',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric_extfreq = SparkOperator(
            task_id='same_value_variant_centric_extfreq',
            doc_md=doc.same_value_variant_centric_extfreq,
            name='etl-qc-same-value-variant-centric-extfreq',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric_ExtFreq',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric_framax = SparkOperator(
            task_id='same_value_variant_centric_framax',
            doc_md=doc.same_value_variant_centric_framax,
            name='etl-qc-same-value-variant-centric-framax',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric_FraMax',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric_freqbyanal = SparkOperator(
            task_id='same_value_variant_centric_freqbyanal',
            doc_md=doc.same_value_variant_centric_freqbyanal,
            name='etl-qc-same-value-variant-centric-freqbyanal',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric_FreqByAnal',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric_freqrqdm = SparkOperator(
            task_id='same_value_variant_centric_freqrqdm',
            doc_md=doc.same_value_variant_centric_freqrqdm,
            name='etl-qc-same-value-variant-centric-freqrqdm',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric_FreqRQDM',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_variant_centric_freqrqdmtumor = SparkOperator(
            task_id='same_value_variant_centric_freqrqdmtumor',
            doc_md=doc.same_value_variant_centric_freqrqdmtumor,
            name='etl-qc-same-value-variant-centric-freqrqdmtumor',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueVariantCentric_FreqRQDMTumor',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_gene_centric = SparkOperator(
            task_id='same_value_gene_centric',
            doc_md=doc.same_value_gene_centric,
            name='etl-qc-same-value-gene-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueGene',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_cnv_centric = SparkOperator(
            task_id='same_value_cnv_centric',
            doc_md=doc.same_value_cnv_centric,
            name='etl-qc-same-value-cnv-centric',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueCNV',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        same_value_coverage_by_gene = SparkOperator(
            task_id='same_value_coverage_by_gene',
            doc_md=doc.same_value_coverage_by_gene,
            name='etl-qc-same-value-coverage-by-gene',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.columncontain.ColumnsContainSameValueCoverageByGene',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        dictionary_cnv = SparkOperator(
            task_id='dictionary_cnv',
            doc_md=doc.dictionary_cnv,
            name='etl-qc-dictionary-cnv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.dictionary.DictionariesCNV',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        dictionary_consequences = SparkOperator(
            task_id='dictionary_consequences',
            doc_md=doc.dictionary_consequences,
            name='etl-qc-dictionary-consequences',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.dictionary.DictionariesConsequences',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        dictionary_donors = SparkOperator(
            task_id='dictionary_donors',
            doc_md=doc.dictionary_donors,
            name='etl-qc-dictionary-donors',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.dictionary.DictionariesDonors',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        dictionary_snv = SparkOperator(
            task_id='dictionary_snv',
            doc_md=doc.dictionary_snv,
            name='etl-qc-dictionary-snv',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.dictionary.DictionariesSNV',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        freq_rqdm_total = SparkOperator(
            task_id='freq_rqdm_total',
            doc_md=doc.freq_rqdm_total,
            name='etl-qc-freq-rqdm-total',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.frequency.RQDMTotal',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        freq_rqdm_affected = SparkOperator(
            task_id='freq_rqdm_affected',
            doc_md=doc.freq_rqdm_affected,
            name='etl-qc-freq-rqdm-affected',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.frequency.RQDMAffected',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        freq_rqdm_non_affected = SparkOperator(
            task_id='freq_rqdm_non_affected',
            doc_md=doc.freq_rqdm_non_affected,
            name='etl-qc-freq-rqdm-non-affected',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.frequency.RQDMNonAffected',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        freq_by_analysis_total = SparkOperator(
            task_id='freq_by_analysis_total',
            doc_md=doc.freq_by_analysis_total,
            name='etl-qc-freq-by-analysis-total',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.frequency.ByAnalysisTotal',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        freq_by_analysis_affected = SparkOperator(
            task_id='freq_by_analysis_affected',
            doc_md=doc.freq_by_analysis_affected,
            name='etl-qc-freq-by-analysis-affected',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.frequency.ByAnalysisAffected',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        freq_by_analysis_non_affected = SparkOperator(
            task_id='freq_by_analysis_non_affected',
            doc_md=doc.freq_by_analysis_non_affected,
            name='etl-qc-freq-by-analysis-non-affected',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.frequency.ByAnalysisNonAffected',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        freq_rqdm_tumor_only = SparkOperator(
            task_id='freq_rqdm_tumor_only',
            doc_md=doc.freq_rqdm_tumor_only,
            name='etl-qc-freq-rqdm-tumor-only',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.frequency.RQDMTumorOnly',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        freq_rqdm_tumor_normal = SparkOperator(
            task_id='freq_rqdm_tumor_normal',
            doc_md=doc.freq_rqdm_tumor_normal,
            name='etl-qc-freq-rqdm-tumor-normal',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.frequency.RQDMTumorNormal',
            spark_config='config-etl-large',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        vcf_snv >> vcf_nor_variants >> vcf_snv_somatic >> filters_snv >> filters_frequency_extra >> filters_frequency_missed >> no_null_variant_centric >> no_null_variant_centric_donors >> no_null_variant_centric_freqbyanal >> no_null_variant_centric_freqrqdm >> no_null_gene_centric >> no_null_cnv_centric >> no_null_coverage_by_gene >> only_null_variant_centric >> only_null_variant_centric_cmc >> only_null_variant_centric_clinvar >> only_null_variant_centric_consequences >> only_null_variant_centric_donors >> only_null_variant_centric_exomax >> only_null_variant_centric_extfreq >> only_null_variant_centric_framax >> only_null_variant_centric_freqbyanal >> only_null_variant_centric_freqrqdm >> only_null_variant_centric_freqrqdmtumor >> only_null_gene_centric >> only_null_cnv_centric >> only_null_coverage_by_gene >> same_value_variant_centric >> same_value_variant_centric_cmc >> same_value_variant_centric_clinvar >> same_value_variant_centric_consequences >> same_value_variant_centric_donors >> same_value_variant_centric_exomax >> same_value_variant_centric_extfreq >> same_value_variant_centric_framax >> same_value_variant_centric_freqbyanal >> same_value_variant_centric_freqrqdm >> same_value_variant_centric_freqrqdmtumor >> same_value_gene_centric >> same_value_cnv_centric >> same_value_coverage_by_gene >> dictionary_cnv >> dictionary_consequences >> dictionary_donors >> dictionary_snv >> freq_rqdm_total >> freq_rqdm_affected >> freq_rqdm_non_affected >> freq_by_analysis_total >> freq_by_analysis_affected >> freq_by_analysis_non_affected >> freq_rqdm_tumor_only >> freq_rqdm_tumor_normal
    return group
