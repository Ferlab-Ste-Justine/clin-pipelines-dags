import logging
from typing import List

from airflow.decorators import task_group, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config
from lib.config import clin_datalake_bucket, env
from lib.franklin import (FranklinStatus, build_s3_analyses_id_key,
                          build_s3_analyses_json_key,
                          build_s3_analyses_status_key,
                          extract_param_from_s3_key, get_completed_analysis,
                          get_franklin_token, write_s3_analysis_status, build_s3_franklin_root_key,
                          get_s3_analyses_keys)
from lib.utils_etl import ClinVCFSuffix
from sensors.franklin import FranklinAPISensor


@task_group(group_id='update')
def franklin_update(analysis_ids: List[str],
                    skip: str,
                    poke_interval: int = 300,
                    timeout: int = 28800  # 8h
                    ):
    api_sensor = FranklinAPISensor(
        task_id='api_sensor',
        analysis_ids=analysis_ids,
        mode='poke',
        soft_fail=False,  # error on failure
        skip=skip,
        poke_interval=poke_interval,
        timeout=timeout,
    )

    @task
    def download_results(_analysis_ids: List[str], _skip: str):
        if _skip:
            raise AirflowSkipException()

        clin_s3 = S3Hook(config.s3_conn_id)
        keys = get_s3_analyses_keys(clin_s3, _analysis_ids)

        token = None
        completed_analyses = []

        did_something = False

        for key in keys:
            if '_FRANKLIN_STATUS_.txt' in key:
                key_obj = clin_s3.get_key(key, clin_datalake_bucket)
                status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                if status is FranklinStatus.READY:  # ignore others status
                    analysis_id = extract_param_from_s3_key(key, 'analysis_id')
                    aliquot_id = extract_param_from_s3_key(key, 'aliquot_id')

                    id_key = clin_s3.get_key(build_s3_analyses_id_key(analysis_id, aliquot_id),
                                             clin_datalake_bucket)
                    id = id_key.get()['Body'].read().decode('utf-8')

                    token = get_franklin_token(token)
                    json = get_completed_analysis(id, token)
                    json_s3_key = build_s3_analyses_json_key(analysis_id, aliquot_id, franklin_analysis_id=id)
                    clin_s3.load_string(json, json_s3_key, clin_datalake_bucket, replace=True)

                    write_s3_analysis_status(clin_s3, analysis_id, aliquot_id, FranklinStatus.COMPLETED)

                    completed_analyses.append(id)
                    logging.info(f'Download JSON: {len(json)} {json_s3_key}')
                    did_something = True

        if not did_something:  # gives a better view during the flow execution than green success
            raise AirflowSkipException('No READY analyses')

        logging.info(f'Completed analyses: {completed_analyses}')

    @task
    def clean_up_clin(_analysis_ids: List[str], _skip: str):
        if _skip:
            raise AirflowSkipException()

        did_something = False
        clin_s3 = S3Hook(config.s3_conn_id)
        keys = get_s3_analyses_keys(clin_s3, _analysis_ids)

        for key in keys:
            if '_FRANKLIN_STATUS_.txt' in key:
                key_obj = clin_s3.get_key(key, clin_datalake_bucket)
                status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                if status is FranklinStatus.COMPLETED:  # ignore others status
                    analysis_id = extract_param_from_s3_key(key, 'analysis_id')
                    aliquot_id = extract_param_from_s3_key(key, 'aliquot_id')

                    # delete _FRANKLIN_STATUS_ and _FRANKLIN_ID_ ...
                    keys_to_delete = [
                        build_s3_analyses_status_key(analysis_id, aliquot_id),
                        build_s3_analyses_id_key(analysis_id, aliquot_id)
                    ]

                    clin_s3.delete_objects(clin_datalake_bucket, keys_to_delete)
                    logging.info(f'Delete: {keys_to_delete}')
                    did_something = True

        if not did_something:
            raise AirflowSkipException('No COMPLETED analyses')

    @task
    def clean_up_franklin(_analysis_ids: List[str], _skip: str):

        if _skip:
            raise AirflowSkipException()

        clin_s3 = S3Hook(config.s3_conn_id)
        keys = get_s3_analyses_keys(clin_s3, _analysis_ids)

        for key in keys:
            if '_FRANKLIN_STATUS_.txt' in key:  # if any status remains then batch isnt completed yet
                raise AirflowSkipException('Not all analyses are COMPLETED')

        did_something = False

        # remove any remaining .txt files such as shared by family _FRANKLIN_IDS_.txt
        for key in keys:
            if '_FRANKLIN_IDS_.txt' in key:
                clin_s3.delete_objects(clin_datalake_bucket, [key])
                logging.info(f'Delete: {key}')
                did_something = True

        franklin_s3 = S3Hook(config.s3_franklin)
        franklin_keys = []

        for analysis_id in _analysis_ids:
            franklin_vcf_root_key = build_s3_franklin_root_key(env, analysis_id)
            franklin_keys += franklin_s3.list_keys(config.s3_franklin_bucket, franklin_vcf_root_key)

        for key in franklin_keys:
            if key.endswith(ClinVCFSuffix.SNV_GERMLINE.value):  # delete all VCFs in Franklin bucket
                franklin_s3.delete_objects(config.s3_franklin_bucket, [key])
                logging.info(f'Delete: {key}')
                did_something = True

        if not did_something:
            raise AirflowSkipException('No COMPLETED analyses')

    (api_sensor >> download_results(analysis_ids, skip) >> clean_up_clin(analysis_ids, skip) >>
     clean_up_franklin(analysis_ids, skip))
