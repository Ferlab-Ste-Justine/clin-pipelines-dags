import logging
from typing import List

from airflow.decorators import task_group, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config
from lib.config import clin_datalake_bucket, clin_phenovar_import_bucket
from lib.phenovar import (
    PhenotypingStatus, build_s3_phenovar_root_key,
    build_s3_result_key, build_s3_status_key, build_s3_task_id_key,
    read_s3_task_id,
    write_s3_analysis_status
)
from sensors.phenovar import PhenotypingAPISensor


@task_group(group_id='update')
def phenovar_update(
    analysis_ids: List[str],
    skip: str,
    poke_interval: int = 300,  # 5 minutes
    timeout: int = 86400  # 24 hours
):
    """
    Task group for finalizing Phenovar analyses.

    Steps:
    1. Sensor: Poll Phenovar API until every analysis completes. On SUCCESS the
       sensor harvests the result JSON from the /check-status response and writes
       it to S3 inline — there is no separate download step.
    2. Clean up clinical: Delete status marker files from datalake
    3. Clean up Phenovar: Delete VCF files from Phenovar import bucket

    Result harvesting lives in the sensor (not a post-sensor task) to close the
    window where a phenovar pod reboot between "sensor sees SUCCESS" and
    "result downloaded" would wipe the celery task meta and lose the result.
    """

    api_sensor = PhenotypingAPISensor(
        task_id='api_sensor',
        analysis_ids=analysis_ids,
        mode='poke',
        soft_fail=False,  # Error on failure
        skip=skip,
        poke_interval=poke_interval,
        timeout=timeout,
    )

    @task
    def clean_up_clin(_analysis_ids: List[str], _skip: str):
        """Delete status marker files from clinical datalake bucket."""
        if _skip:
            raise AirflowSkipException()
        
        clin_s3 = S3Hook(config.s3_conn_id)
        did_something = False
        
        for analysis_id in _analysis_ids:
            status_key = build_s3_status_key(analysis_id)
            
            if not clin_s3.check_for_key(status_key, clin_datalake_bucket):
                continue
            
            key_obj = clin_s3.get_key(status_key, clin_datalake_bucket)
            status = PhenotypingStatus[key_obj.get()['Body'].read().decode('utf-8')]
            
            # Only clean up if we have a result or failure
            if status in [PhenotypingStatus.SUCCESS, PhenotypingStatus.FAILURE]:
                result_key = build_s3_result_key(analysis_id)
                
                # For SUCCESS, only clean up if result exists
                if status == PhenotypingStatus.SUCCESS:
                    if not clin_s3.check_for_key(result_key, clin_datalake_bucket):
                        logging.warning(f'Result file missing for analysis {analysis_id}, skipping cleanup')
                        continue
                
                # Delete marker files
                keys_to_delete = [
                    build_s3_status_key(analysis_id),
                    build_s3_task_id_key(analysis_id)
                ]
                
                clin_s3.delete_objects(clin_datalake_bucket, keys_to_delete)
                logging.info(f'Deleted marker files for {analysis_id}: {keys_to_delete}')
                did_something = True
        
        if not did_something:
            raise AirflowSkipException('No analyses ready for cleanup')
    
    @task
    def clean_up_phenovar(_analysis_ids: List[str], _skip: str):
        """Delete VCF files from Phenovar import bucket."""
        if _skip:
            raise AirflowSkipException()
        
        phenovar_s3 = S3Hook(config.s3_conn_id)
        did_something = False
        
        # Delete VCF files from Phenovar import bucket
        for analysis_id in _analysis_ids:
            phenovar_prefix = f'{analysis_id}/'
            vcf_keys = phenovar_s3.list_keys(clin_phenovar_import_bucket, phenovar_prefix)
            
            if vcf_keys:
                # Only delete VCF files
                vcf_files = [k for k in vcf_keys if k.endswith('.vcf') or k.endswith('.vcf.gz')]
                if vcf_files:
                    phenovar_s3.delete_objects(clin_phenovar_import_bucket, vcf_files)
                    logging.info(f'Deleted {len(vcf_files)} VCF files for analysis {analysis_id}')
                    did_something = True
        
        if not did_something:
            raise AirflowSkipException('No VCF files to delete')
    
    # Chain the tasks
    (api_sensor >> clean_up_clin(analysis_ids, skip) >> clean_up_phenovar(analysis_ids, skip))
