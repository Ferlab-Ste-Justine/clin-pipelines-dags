import logging

from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup

from lib import config
from lib.config import (clin_datalake_bucket,
                        env)
from lib.franklin import (FranklinStatus, build_s3_analyses_id_key, build_s3_analyses_json_key,
                          build_s3_analyses_status_key,
                          extract_param_from_s3_key, get_completed_analysis, get_franklin_token,
                          write_s3_analysis_status)
from lib.utils_etl import (ClinVCFSuffix)
from sensors.franklin import FranklinAPISensor


def FranklinUpdate(
    group_id: str,
    batch_id: str,
    skip: str,
    poke_interval = 300,
    timeout = 3600
) -> TaskGroup:

    with TaskGroup(group_id=group_id) as group:

        def download_results(batch_id, skip):
            
            if skip:
                raise AirflowSkipException()

            clin_s3 = S3Hook(config.s3_conn_id)
            keys = clin_s3.list_keys(clin_datalake_bucket, f'raw/landing/franklin/batch_id={batch_id}/')

            token = None
            completed_analyses = []

            did_something = False

            for key in keys:
                if '_FRANKLIN_STATUS_.txt' in key:
                    key_obj = clin_s3.get_key(key, clin_datalake_bucket)
                    status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                    if status is FranklinStatus.READY:    # ignore others status
                        family_id = extract_param_from_s3_key(key, 'family_id') 
                        aliquot_id = extract_param_from_s3_key(key, 'aliquot_id')

                        id_key = clin_s3.get_key(build_s3_analyses_id_key(batch_id, family_id, aliquot_id), clin_datalake_bucket)
                        id = id_key.get()['Body'].read().decode('utf-8')

                        token = get_franklin_token(token)
                        json = get_completed_analysis(id, token)
                        json_s3_key = build_s3_analyses_json_key(batch_id, family_id, aliquot_id, id)
                        clin_s3.load_string(json, json_s3_key, clin_datalake_bucket, replace=True)

                        write_s3_analysis_status(clin_s3, batch_id, family_id, aliquot_id, FranklinStatus.COMPLETED)

                        completed_analyses.append(id)
                        logging.info(f'Download JSON: {len(json)} {json_s3_key}')
                        did_something = True
            
            if not did_something:  # gives a better view during the flow execution than green success
                raise AirflowSkipException('No READY analyses')
            
            logging.info(f'Completed analyses: {completed_analyses}')
        
        def clean_up_clin(batch_id, skip):

            if skip:
                raise AirflowSkipException()

            did_something = False
            clin_s3 = S3Hook(config.s3_conn_id)
            keys = clin_s3.list_keys(clin_datalake_bucket, f'raw/landing/franklin/batch_id={batch_id}/')
            for key in keys:
                if '_FRANKLIN_STATUS_.txt' in key:
                    key_obj = clin_s3.get_key(key, clin_datalake_bucket)
                    status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                    if status is FranklinStatus.COMPLETED:    # ignore others status
                        family_id = extract_param_from_s3_key(key, 'family_id') 
                        aliquot_id = extract_param_from_s3_key(key, 'aliquot_id')

                        keys_to_delete = [] # delete _FRANKLIN_STATUS_ and _FRANKLIN_ID_ ...
                        keys_to_delete.append(build_s3_analyses_status_key(batch_id, family_id, aliquot_id))
                        keys_to_delete.append(build_s3_analyses_id_key(batch_id, family_id, aliquot_id))

                        clin_s3.delete_objects(clin_datalake_bucket, keys_to_delete)
                        logging.info(f'Delete: {keys_to_delete}')
                        did_something = True

            if not did_something:
                raise AirflowSkipException('No COMPLETED analyses')

        def clean_up_franklin(batch_id, skip):

            if skip:
                raise AirflowSkipException()

            clin_s3 = S3Hook(config.s3_conn_id)
            keys = clin_s3.list_keys(clin_datalake_bucket, f'raw/landing/franklin/batch_id={batch_id}/')
            for key in keys:
                if '_FRANKLIN_STATUS_.txt' in key:  # if any status remains then batch isnt completed yet
                    raise AirflowSkipException('Not all analyses are COMPLETED')

            did_something = False

            # remove any remaining .txt files such as shared by family _FRANKLIN_IDS_.txt
            keys = clin_s3.list_keys(clin_datalake_bucket, f'raw/landing/franklin/batch_id={batch_id}/')
            for key in keys:
                if '_FRANKLIN_IDS_.txt' in key:
                    clin_s3.delete_objects(clin_datalake_bucket, [key])
                    logging.info(f'Delete: {key}')
                    did_something = True

            franklin_s3 = S3Hook(config.s3_franklin)
            keys = franklin_s3.list_keys(config.s3_franklin_bucket, f'{env}/{batch_id}')
            for key in keys:
                if key.endswith(ClinVCFSuffix.SNV_GERMLINE.value): # delete all VCFs in Franklin bucket
                    franklin_s3.delete_objects(config.s3_franklin_bucket, [key])
                    logging.info(f'Delete: {key}')
                    did_something = True
            
            if not did_something:
                raise AirflowSkipException('No COMPLETED analyses')

        api_sensor = FranklinAPISensor(
            task_id='api_sensor',
            batch_id=batch_id,
            mode='poke',
            soft_fail=True, # SKIP on failure
            skip=skip,
            poke_interval=poke_interval,
            timeout=timeout,
        )

        download_results = PythonOperator(
            task_id='download_results',
            op_args=[batch_id, skip],
            python_callable=download_results,
        )

        clean_up_clin = PythonOperator(
            task_id='clean_up_clin',
            op_args=[batch_id, skip],
            python_callable=clean_up_clin,
        )

        clean_up_franklin = PythonOperator(
            task_id='clean_up_franklin',
            op_args=[batch_id, skip],
            python_callable=clean_up_franklin,
        )

        api_sensor >> download_results >> clean_up_clin >> clean_up_franklin

    return group
