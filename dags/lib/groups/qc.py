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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )
        '''
        variants_should_be_annotated = SparkOperator(
            task_id='variants_should_be_annotated',
            doc_md=doc.variants_should_be_annotated,
            name='etl-qc-variants-should-be-annotated',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.varsome.VariantsShouldBeAnnotated',
            spark_config='config-etl-small',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        variants_should_not_be_annotated = SparkOperator(
            task_id='variants_should_not_be_annotated',
            doc_md=doc.variants_should_not_be_annotated,
            name='etl-qc-variants-should-not-be-annotated',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.varsome.VariantsShouldNotBeAnnotated',
            spark_config='config-etl-small',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        variants_should_be_reannotated = SparkOperator(
            task_id='variants_should_be_reannotated',
            doc_md=doc.variants_should_be_reannotated,
            name='etl-qc-variants-should-be-reannotated',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.varsome.VariantsShouldBeReannotated',
            spark_config='config-etl-small',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        variants_should_not_be_reannotated = SparkOperator(
            task_id='variants_should_not_be_reannotated',
            doc_md=doc.variants_should_not_be_reannotated,
            name='etl-qc-variants-should-not-be-reannotated',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.varsome.VariantsShouldNotBeReannotated',
            spark_config='config-etl-small',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )
        '''

        freq_rqdm_total = SparkOperator(
            task_id='freq_rqdm_total',
            doc_md=doc.freq_rqdm_total,
            name='etl-qc-freq-rqdm-total',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.qc.frequency.RQDMTotal',
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
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
            spark_config='config-etl-small',
            spark_jar=spark_jar,
            arguments=['clin' + env_url('_')],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        [vcf_snv, vcf_nor_variants, filters_snv, filters_frequency_extra, filters_frequency_missed, no_null_variant_centric, no_null_gene_centric, no_null_cnv_centric, only_null_variant_centric, only_null_gene_centric, only_null_cnv_centric, same_value_variant_centric, same_value_gene_centric, same_value_cnv_centric, dictionary_cnv, dictionary_consequences, dictionary_donors, dictionary_snv, freq_rqdm_total, freq_rqdm_affected, freq_rqdm_non_affected, freq_by_analysis_total, freq_by_analysis_affected, freq_by_analysis_non_affected]
    return group
