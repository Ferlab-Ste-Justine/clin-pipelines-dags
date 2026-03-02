import logging
from typing import List

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator

from lib import config
from lib.config import clin_datalake_bucket
from lib.phenovar import (
    PhenotypingStatus, build_s3_phenovar_root_key, 
    check_phenovar_status, read_s3_task_id,
    write_s3_analysis_status
)


class PhenotypingAPISensor(BaseSensorOperator):
    """
    Sensor that polls Phenovar API for analysis completion.
    Checks analyses with PENDING or STARTED status and updates them to SUCCESS or FAILURE.
    """
    
    template_fields = BaseSensorOperator.template_fields + (
        'skip', 'analysis_ids',
    )

    def __init__(self, analysis_ids: List[str], skip: bool = False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.skip = skip
        self.analysis_ids = analysis_ids

    def poke(self, context):
        if self.skip:
            raise AirflowSkipException()

        analysis_ids = self.analysis_ids
        clin_s3 = S3Hook(config.s3_conn_id)
        
        # Find analyses with PENDING or STARTED status
        pending_analyses = []
        analysis_to_task_id = {}
        
        for analysis_id in analysis_ids:
            status_key = f'{build_s3_phenovar_root_key(analysis_id)}/_PHENOVAR_STATUS_.txt'
            
            if clin_s3.check_for_key(status_key, clin_datalake_bucket):
                key_obj = clin_s3.get_key(status_key, clin_datalake_bucket)
                status = PhenotypingStatus[key_obj.get()['Body'].read().decode('utf-8')]
                
                if status in [PhenotypingStatus.PENDING, PhenotypingStatus.STARTED]:
                    logging.info(f'Found {status.name} analysis: {analysis_id}')
                    pending_analyses.append(analysis_id)
                    
                    # Read task ID for this analysis
                    task_id = read_s3_task_id(clin_s3, analysis_id)
                    if task_id:
                        analysis_to_task_id[analysis_id] = task_id
                    else:
                        logging.warning(f'No task ID found for analysis {analysis_id}')
        
        pending_count = len(pending_analyses)
        
        if pending_count == 0:
            raise AirflowSkipException('No PENDING or STARTED analyses')
        
        logging.info(f'Checking status for {pending_count} analyses')
        
        # Check status for each task (in chunks of 10 to avoid overwhelming the API)
        completed_analyses = []
        chunk_size = 10
        
        for i in range(0, len(pending_analyses), chunk_size):
            chunk = pending_analyses[i:i + chunk_size]
            
            for analysis_id in chunk:
                task_id = analysis_to_task_id.get(analysis_id)
                if not task_id:
                    continue
                
                try:
                    status_response = check_phenovar_status(task_id)
                    phenovar_status = status_response.get('status', 'UNKNOWN')
                    
                    logging.info(f'Analysis {analysis_id} (task {task_id}): {phenovar_status}')
                    
                    if phenovar_status == 'SUCCESS':
                        write_s3_analysis_status(clin_s3, analysis_id, PhenotypingStatus.SUCCESS)
                        completed_analyses.append(analysis_id)
                    elif phenovar_status == 'FAILURE':
                        write_s3_analysis_status(clin_s3, analysis_id, PhenotypingStatus.FAILURE)
                        completed_analyses.append(analysis_id)
                    elif phenovar_status in ['PENDING', 'STARTED']:
                        # Update to STARTED if it was PENDING
                        write_s3_analysis_status(clin_s3, analysis_id, PhenotypingStatus.STARTED)
                    else:
                        logging.warning(f'Unknown Phenovar status: {phenovar_status} for task {task_id}')
                        
                except Exception as e:
                    logging.error(f'Error checking status for analysis {analysis_id}: {str(e)}')
        
        completed_count = len(completed_analyses)
        logging.info(f'Completed analyses: {completed_count}/{pending_count}')
        
        # Return True when all pending analyses are completed (SUCCESS or FAILURE)
        return completed_count == pending_count
