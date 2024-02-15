import logging

from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup

from lib import config
from lib.config import (clin_import_bucket)
from lib.franklin import (FranklinStatus, attach_vcf_to_analyses,
                          can_create_analysis, extract_vcf_prefix,
                          filter_valid_families, get_franklin_token,
                          group_families_from_metadata,
                          post_create_analysis, transfer_vcf_to_franklin,
                          write_s3_analyses_status)
from lib.utils_etl import (ClinSchema, ClinVCFSuffix, get_metadata_content)


def FranklinCreate(
    group_id: str,
    batch_id: str,
    skip: str,
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        def validate_task(batch_id, skip):
            clin_s3 = S3Hook(config.s3_conn_id)
            metadata = get_metadata_content(clin_s3, batch_id)
            submission_schema = metadata.get('submissionSchema', '')
            logging.info(f'Skip: {skip} Schema: {submission_schema}')
            return not skip and submission_schema == ClinSchema.GERMLINE.value # only GERMLINE

        '''
        Some rules about tasks:
        - Can't use AirflowSkipException with Task
        - Tasks should always return something
        '''

        @task
        def group_families(batch_id, skip):
            if validate_task(batch_id, skip):
                clin_s3 = S3Hook(config.s3_conn_id)
                metadata = get_metadata_content(clin_s3, batch_id)
                [grouped_by_families, without_families] = group_families_from_metadata(metadata)
                filtered_families = filter_valid_families(grouped_by_families)
                logging.info(filtered_families)
                return {'families': filtered_families, 'no_family': without_families}
            return {}

        @task
        def vcf_to_analyses(families, batch_id, skip):
            if validate_task(batch_id, skip):
                clin_s3 = S3Hook(config.s3_conn_id)
                vcfs = {}
                keys = clin_s3.list_keys(clin_import_bucket, f'{batch_id}/')
                for key in keys:
                    if key.endswith(ClinVCFSuffix.SNV_GERMLINE.value):
                        vcfs[key] = extract_vcf_prefix(key)
                return attach_vcf_to_analyses(families, vcfs) # we now have analysis <=> vcf
            return {}

        @task
        def create_analyses(families, batch_id, skip):
            if validate_task(batch_id, skip):
                clin_s3 = S3Hook(config.s3_conn_id)
                franklin_s3 = S3Hook(config.s3_franklin)

                created_ids = []
                token = None

                for family_id, analyses in families['families'].items():
                    if (can_create_analysis(clin_s3, batch_id, family_id, analyses)): # already created before
                        token = get_franklin_token(token)
                        transfer_vcf_to_franklin(clin_s3, franklin_s3, analyses)
                        ids = post_create_analysis(family_id, analyses, token, clin_s3, franklin_s3, batch_id)
                        write_s3_analyses_status(clin_s3, batch_id, family_id, analyses, FranklinStatus.CREATED, ids)
                        created_ids += ids
                
                for patient in families['no_family']:
                    analyses = [patient]
                    if (can_create_analysis(clin_s3, batch_id, None, analyses)): # already created before
                        token = get_franklin_token(token)
                        transfer_vcf_to_franklin(clin_s3, franklin_s3, analyses)
                        ids = post_create_analysis(None, analyses, token, clin_s3, franklin_s3, batch_id)
                        write_s3_analyses_status(clin_s3, batch_id, None, analyses, FranklinStatus.CREATED, ids)
                        created_ids += ids

                logging.info(created_ids)
                return created_ids # success !!!
            return {}

        create_analyses(
            vcf_to_analyses(
                group_families(
                    batch_id, skip), 
                batch_id, skip),
            batch_id, skip), 

    return group
