import logging
from typing import List

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config
from lib.datasets import enriched_clinical
from lib.franklin import (FranklinStatus, attach_vcf_to_analyses,
                          can_create_analysis, get_franklin_token,
                          post_create_analysis, transfer_vcf_to_franklin,
                          write_s3_analyses_status)


@task_group(group_id='create')
def franklin_create(analysis_ids: List[str], skip: str):
    @task.virtualenv(requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
    def group_families(_analysis_ids: List[str], _skip: str) -> dict:
        # Local imports for virtualenv
        import logging
        from lib.franklin import get_clinical_data, group_families_from_clinical_data, filter_valid_families

        if _skip:
            raise AirflowSkipException()

        clinical_data: List[dict] = get_clinical_data(_analysis_ids)
        family_groups, solo_analyses = group_families_from_clinical_data(clinical_data)
        filtered_families = filter_valid_families(family_groups)
        logging.info(filtered_families)
        return {'families': filtered_families, 'no_family': solo_analyses}

    @task
    def vcf_to_analyses(families: dict, _analysis_ids: List[str], _skip: str):
        if _skip:
            raise AirflowSkipException()

        clin_s3 = S3Hook(config.s3_conn_id)
        return attach_vcf_to_analyses(clin_s3, families)  # we now have analysis <=> vcf

    @task
    def create_analyses(families, _skip):
        if _skip:
            raise AirflowSkipException()

        clin_s3 = S3Hook(config.s3_conn_id)
        franklin_s3 = S3Hook(config.s3_franklin)

        created_ids = []
        token = None

        for family_id, analyses in families['families'].items():
            if can_create_analysis(clin_s3, analyses):  # already created before
                token = get_franklin_token(token)
                transfer_vcf_to_franklin(clin_s3, franklin_s3, analyses)
                ids = post_create_analysis(family_id, analyses, token, franklin_s3)
                write_s3_analyses_status(clin_s3, analyses, FranklinStatus.CREATED, ids)
                created_ids += ids

        for patient in families['no_family']:
            analyses = [patient]
            if can_create_analysis(clin_s3, analyses):  # already created before
                token = get_franklin_token(token)
                transfer_vcf_to_franklin(clin_s3, franklin_s3, analyses)
                ids = post_create_analysis(None, analyses, token, franklin_s3)
                write_s3_analyses_status(clin_s3, analyses, FranklinStatus.CREATED, ids)
                created_ids += ids

        logging.info(created_ids)
        return created_ids  # success !!!

    create_analyses(
        vcf_to_analyses(
            group_families(
                analysis_ids, skip),
            analysis_ids, skip),
        skip),
