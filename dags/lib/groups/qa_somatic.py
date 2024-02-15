from airflow.decorators import task_group

from lib.tasks import qa


@task_group(group_id='qa')
def qa_somatic(
        release_id: str,
        spark_jar: str
):
    """
    Runs QA tasks for somatic releases.
    """
    non_empty_tables = qa.non_empty_tables(release_id, spark_jar)
    no_dup_gnomad = qa.no_dup_gnomad(release_id, spark_jar)
    no_dup_nor_snv_somatic = qa.no_dup_nor_snv_somatic(release_id, spark_jar)
    no_dup_nor_consequences = qa.no_dup_nor_consequences(release_id, spark_jar)
    no_dup_nor_variants = qa.no_dup_nor_variants(release_id, spark_jar)
    no_dup_snv_somatic = qa.no_dup_snv_somatic(release_id, spark_jar)
    no_dup_consequences = qa.no_dup_consequences(release_id, spark_jar)
    no_dup_variants = qa.no_dup_variants(release_id, spark_jar)
    no_dup_variant_centric = qa.no_dup_variant_centric(release_id, spark_jar)
    no_dup_cnv_centric = qa.no_dup_cnv_centric(release_id, spark_jar)
    same_list_nor_snv_somatic_nor_variants = qa.same_list_nor_snv_somatic_nor_variants(release_id, spark_jar)
    same_list_snv_somatic_variants = qa.same_list_snv_somatic_variants(release_id, spark_jar)
    same_list_variants_variant_centric = qa.same_list_variants_variant_centric(release_id, spark_jar)

    [non_empty_tables, no_dup_gnomad, no_dup_nor_snv_somatic, no_dup_nor_consequences, no_dup_nor_variants,
     no_dup_snv_somatic, no_dup_consequences, no_dup_variants, no_dup_variant_centric, no_dup_cnv_centric,
     same_list_nor_snv_somatic_nor_variants, same_list_snv_somatic_variants, same_list_variants_variant_centric]
