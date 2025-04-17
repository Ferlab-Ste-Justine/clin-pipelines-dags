from airflow.decorators import task_group
from lib.tasks import qa as qa_tasks


@task_group(group_id='qa')
def qa(
        spark_jar: str,
):
    """
    Run all QA tasks.
    """
    non_empty_tables = qa_tasks.non_empty_tables(spark_jar)
    no_dup_gnomad_joint = qa_tasks.no_dup_gnomad_joint(spark_jar)
    no_dup_clinical = qa_tasks.no_dup_clinical(spark_jar)
    no_dup_nor_snv = qa_tasks.no_dup_nor_snv(spark_jar)
    no_dup_nor_snv_somatic = qa_tasks.no_dup_nor_snv_somatic(spark_jar)
    no_dup_nor_consequences = qa_tasks.no_dup_nor_consequences(spark_jar)
    no_dup_nor_variants = qa_tasks.no_dup_nor_variants(spark_jar)
    no_dup_snv = qa_tasks.no_dup_snv(spark_jar)
    no_dup_snv_somatic = qa_tasks.no_dup_snv_somatic(spark_jar)
    no_dup_consequences = qa_tasks.no_dup_consequences(spark_jar)
    no_dup_variants = qa_tasks.no_dup_variants(spark_jar)
    no_dup_variant_centric = qa_tasks.no_dup_variant_centric(spark_jar)
    no_dup_cnv_centric = qa_tasks.no_dup_cnv_centric(spark_jar)
    no_dup_coverage_by_gene = qa_tasks.no_dup_coverage_by_gene(spark_jar)
    no_dup_nextflow_svclustering = qa_tasks.no_dup_nextflow_svclustering(spark_jar)
    no_dup_nextflow_svclustering_parental_origin = qa_tasks.no_dup_nextflow_svclustering_parental_origin(spark_jar)
    same_list_nor_snv_nor_variants = qa_tasks.same_list_nor_snv_nor_variants(spark_jar)
    same_list_nor_snv_somatic_nor_variants = qa_tasks.same_list_nor_snv_somatic_nor_variants(spark_jar)
    same_list_snv_variants = qa_tasks.same_list_snv_variants(spark_jar)
    same_list_snv_somatic_variants = qa_tasks.same_list_snv_somatic_variants(spark_jar)
    same_list_variants_variant_centric = qa_tasks.same_list_variants_variant_centric(spark_jar)

    [non_empty_tables, no_dup_gnomad_joint, no_dup_clinical, no_dup_nor_snv, no_dup_nor_snv_somatic, no_dup_nor_consequences,
     no_dup_nor_variants, no_dup_snv, no_dup_snv_somatic, no_dup_consequences, no_dup_variants,
     no_dup_variant_centric, no_dup_cnv_centric, no_dup_coverage_by_gene, no_dup_nextflow_svclustering,
     no_dup_nextflow_svclustering_parental_origin, same_list_nor_snv_nor_variants,
     same_list_nor_snv_somatic_nor_variants, same_list_snv_variants, same_list_snv_somatic_variants,
     same_list_variants_variant_centric]
