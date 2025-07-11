import logging
from typing import List

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator

from lib import config
from lib.config import clin_datalake_bucket
from lib.franklin import (FranklinStatus, build_s3_analyses_ids_key,
                          extract_from_name_aliquot_id,
                          extract_param_from_s3_key, get_analysis_status,
                          get_franklin_token, write_s3_analysis_status, get_s3_analyses_keys,
                          extract_from_name_analysis_id)


class FranklinAPISensor(BaseSensorOperator):
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
        keys = get_s3_analyses_keys(clin_s3, analysis_ids)

        created_analyses = []
        ready_analyses = []

        for key in keys:
            if '_FRANKLIN_STATUS_.txt' in key:
                key_obj = clin_s3.get_key(key, clin_datalake_bucket)
                status = FranklinStatus[key_obj.get()['Body'].read().decode('utf-8')]
                if status is FranklinStatus.CREATED:  # ignore others status

                    logging.info(f'Found CREATED: {key}')
                    analysis_id = extract_param_from_s3_key(key, 'analysis_id')

                    ids_key = clin_s3.get_key(build_s3_analyses_ids_key(analysis_id), clin_datalake_bucket)
                    ids = ids_key.get()['Body'].read().decode('utf-8').split(',')

                    created_analyses += ids

        # remove duplicated IDs if any
        created_analyses = list(set(created_analyses))
        created_count = len(created_analyses)

        if created_count == 0:
            raise AirflowSkipException('No CREATED analyses')

        token = get_franklin_token()
        statuses = get_analysis_status(created_analyses, token)
        for status in statuses:
            if status['processing_status'] == 'READY':

                franklin_analysis_id = str(status['id'])

                if franklin_analysis_id in created_analyses:
                    analysis_id = extract_from_name_analysis_id(status['name'])
                    analysis_aliquot_id = extract_from_name_aliquot_id(status['name'])

                    write_s3_analysis_status(clin_s3, analysis_id, analysis_aliquot_id, FranklinStatus.READY,
                                             id=franklin_analysis_id)

                    ready_analyses.append(franklin_analysis_id)
                else:
                    logging.warning(f'Unexpected franklin analysis ID: {franklin_analysis_id} in status response')

        ready_count = len(ready_analyses)

        logging.info(f'Ready analyses: {ready_count}/{created_count} {ready_analyses}')
        return ready_count == created_count  # All created analyses are ready
