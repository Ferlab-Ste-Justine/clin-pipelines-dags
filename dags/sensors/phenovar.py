import json
import logging
from typing import List

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator

from lib import config
from lib.config import clin_datalake_bucket
from lib.phenovar import (
    PHENOVAR_BOOT_ID_XCOM_KEY,
    PhenotypingStatus, build_s3_result_key, build_s3_status_key,
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
        
        # Phenovar pod boot_id captured at submit time (same for all analyses in this DAG run).
        # Empty string when the DAG run predates the boot_id mechanism — comparison is skipped.
        submit_boot_id = context['ti'].xcom_pull(
            task_ids='create.submit_analyses',
            key=PHENOVAR_BOOT_ID_XCOM_KEY,
        ) or ''

        # Find analyses with PENDING, STARTED, or FAILURE status
        pending_analyses = []
        failed_analyses_existing = []
        analysis_to_task_id = {}

        for analysis_id in analysis_ids:
            status_key =  build_s3_status_key(analysis_id)

            if clin_s3.check_for_key(status_key, clin_datalake_bucket):
                key_obj = clin_s3.get_key(status_key, clin_datalake_bucket)
                status = PhenotypingStatus[key_obj.get()['Body'].read().decode('utf-8')]

                # Crash-recovery shortcut: if a prior sensor run already wrote the result
                # JSON but crashed before flipping the marker to SUCCESS, just flip it now.
                # The result is durably on S3 — no need to re-poll phenovar (where the
                # task_id may have been wiped by a meanwhile-happened pod reboot).
                if (
                    status in [PhenotypingStatus.PENDING, PhenotypingStatus.STARTED]
                    and clin_s3.check_for_key(build_s3_result_key(analysis_id), clin_datalake_bucket)
                ):
                    logging.info(
                        f'Analysis {analysis_id}: result file already on S3 '
                        f'(marker was {status.name}); marking SUCCESS'
                    )
                    write_s3_analysis_status(clin_s3, analysis_id, PhenotypingStatus.SUCCESS)
                    continue

                if status in [PhenotypingStatus.PENDING, PhenotypingStatus.STARTED]:
                    logging.info(f'Found {status.name} analysis: {analysis_id}')
                    pending_analyses.append(analysis_id)

                    # Read task ID for this analysis
                    task_id = read_s3_task_id(clin_s3, analysis_id)
                    if task_id:
                        analysis_to_task_id[analysis_id] = task_id
                    else:
                        logging.warning(f'No task ID found for analysis {analysis_id}')
                elif status == PhenotypingStatus.FAILURE:
                    logging.error(f'Found existing FAILURE analysis: {analysis_id}')
                    failed_analyses_existing.append(analysis_id)
        
        pending_count = len(pending_analyses)
        
        # Fail if any analyses have already failed
        if failed_analyses_existing:
            failure_details = '\n'.join([f'  - {analysis_id}' for analysis_id in failed_analyses_existing])
            raise AirflowFailException(
                f'Phenovar analysis already failed for {len(failed_analyses_existing)} analysis:\n{failure_details}'
            )
        
        if pending_count == 0:
            raise AirflowSkipException('No PENDING or STARTED analyses')
        
        logging.info(f'Checking status for {pending_count} analyses')
        
        # Check status for each task (in chunks of 10 to avoid overwhelming the API)
        completed_analyses = []
        failed_analyses = []
        chunk_size = 10
        
        for i in range(0, len(pending_analyses), chunk_size):
            chunk = pending_analyses[i:i + chunk_size]
            
            for analysis_id in chunk:
                task_id = analysis_to_task_id.get(analysis_id)
                if not task_id:
                    continue
                
                try:
                    status_response = check_phenovar_status(task_id)
                    phenovar_state = status_response.get('state', 'UNKNOWN')
                    message = status_response.get('message', '')
                    current_boot_id = status_response.get('boot_id', '')

                    logging.info(f'Analysis {analysis_id} (task {task_id}): {phenovar_state}')

                    # Detect pod reset that orphaned this task: submit-time boot_id mismatches current.
                    # Skip when either side is empty (pre-boot_id DAG runs or phenovar without the field).
                    if submit_boot_id and current_boot_id and submit_boot_id != current_boot_id:
                        orphan_msg = (
                            f'pod reset mid-analysis '
                            f'(boot_id changed {submit_boot_id}→{current_boot_id})'
                        )
                        logging.error(f'Analysis {analysis_id} orphaned: {orphan_msg}')
                        write_s3_analysis_status(clin_s3, analysis_id, PhenotypingStatus.FAILURE)
                        failed_analyses.append((analysis_id, orphan_msg))
                    elif phenovar_state == 'SUCCESS':
                        # Harvest the result JSON inline from the /check-status payload.
                        # Writing result BEFORE flipping the marker to SUCCESS guarantees
                        # that any subsequent pod reboot (which would wipe the redis
                        # task meta) can't strand us in a state where the marker says
                        # SUCCESS but the result is lost. If the write-marker step below
                        # crashes, the next poke's crash-recovery shortcut handles it.
                        result_blob = status_response.get('result')
                        if not result_blob:
                            raise AirflowFailException(
                                f'Analysis {analysis_id}: SUCCESS response missing result payload'
                            )
                        result_json = json.dumps(result_blob)
                        result_key = build_s3_result_key(analysis_id)
                        clin_s3.load_string(
                            result_json, result_key, clin_datalake_bucket, replace=True
                        )
                        logging.info(
                            f'Analysis {analysis_id}: wrote {len(result_json)} bytes '
                            f'of result to {result_key}'
                        )
                        write_s3_analysis_status(clin_s3, analysis_id, PhenotypingStatus.SUCCESS)
                        completed_analyses.append(analysis_id)
                    elif phenovar_state == 'FAILURE':
                        logging.error(f'Analysis {analysis_id} failed in Phenovar: {message}')
                        write_s3_analysis_status(clin_s3, analysis_id, PhenotypingStatus.FAILURE)
                        failed_analyses.append((analysis_id, message))
                    elif phenovar_state in ['PENDING', 'STARTED', 'RECEIVED']:
                        # Update to STARTED if it was PENDING
                        write_s3_analysis_status(clin_s3, analysis_id, PhenotypingStatus.STARTED)
                    else:
                        logging.warning(f'Unknown Phenovar state: {phenovar_state} for task {task_id}')

                except AirflowFailException:
                    # Don't swallow our own fail signals (boot-id mismatch, FAILURE path above,
                    # or hard API errors surfaced by parse_response).
                    raise
                except Exception as e:
                    # Transient network/parse error — log and let the next poke retry.
                    logging.warning(f'Transient error checking status for analysis {analysis_id}: {str(e)}')
        
        # If any analyses failed, fail the task immediately
        if failed_analyses:
            failure_details = '\n'.join([f'  - {analysis_id}: {msg}' for analysis_id, msg in failed_analyses])
            raise AirflowFailException(
                f'Phenovar analysis failed for {len(failed_analyses)} analysis:\n{failure_details}'
            )
        
        completed_count = len(completed_analyses)
        logging.info(f'Completed analyses: {completed_count}/{pending_count}')
        
        # Return True when all pending analyses are completed successfully
        return completed_count == pending_count
