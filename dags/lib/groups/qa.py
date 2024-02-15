from airflow.decorators import task_group

from lib.tasks import qa as qa_tasks


@task_group(group_id='qa')
def qa(
        release_id: str,
        spark_jar: str,
):
    """
    Runs all QA tasks.
    """
    non_empty_tables = qa_tasks.non_empty_tables(release_id, spark_jar)
    no_dup_gnomad = qa_tasks.no_dup_gnomad(release_id, spark_jar)
    no_dup_nor_snv = qa_tasks.no_dup_nor_snv(release_id, spark_jar)
    no_dup_nor_snv_somatic = qa_tasks.no_dup_nor_snv_somatic(release_id, spark_jar)
    no_dup_nor_consequences = qa_tasks.no_dup_nor_consequences(release_id, spark_jar)
    no_dup_nor_variants = qa_tasks.no_dup_nor_variants(release_id, spark_jar)
    no_dup_snv = qa_tasks.no_dup_snv(release_id, spark_jar)
    no_dup_snv_somatic = qa_tasks.no_dup_snv_somatic(release_id, spark_jar)
    no_dup_consequences = qa_tasks.no_dup_consequences(release_id, spark_jar)
    no_dup_variants = qa_tasks.no_dup_variants(release_id, spark_jar)
    no_dup_variant_centric = qa_tasks.no_dup_variant_centric(release_id, spark_jar)
    no_dup_cnv_centric = qa_tasks.no_dup_cnv_centric(release_id, spark_jar)
    # no_dup_varsome = qa_tasks.no_dup_varsome(release_id, spark_jar)
    same_list_nor_snv_nor_variants = qa_tasks.same_list_nor_snv_nor_variants(release_id, spark_jar)
    same_list_nor_snv_somatic_nor_variants = qa_tasks.same_list_nor_snv_somatic_nor_variants(release_id, spark_jar)
    same_list_snv_variants = qa_tasks.same_list_snv_variants(release_id, spark_jar)
    same_list_snv_somatic_variants = qa_tasks.same_list_snv_somatic_variants(release_id, spark_jar)
    same_list_variants_variant_centric = qa_tasks.same_list_variants_variant_centric(release_id, spark_jar)

    [non_empty_tables, no_dup_gnomad, no_dup_nor_snv, no_dup_nor_snv_somatic, no_dup_nor_consequences,
     no_dup_nor_variants, no_dup_snv, no_dup_snv_somatic, no_dup_consequences, no_dup_variants,
     no_dup_variant_centric, no_dup_cnv_centric, same_list_nor_snv_nor_variants,
     same_list_nor_snv_somatic_nor_variants, same_list_snv_variants, same_list_snv_somatic_variants,
     same_list_variants_variant_centric]
