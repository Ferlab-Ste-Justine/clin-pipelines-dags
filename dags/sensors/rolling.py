import logging

import requests
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sensors.base import BaseSensorOperator
from lib.config import env, es_url
from lib.slack import Slack
from lib.utils_es import color, format_es_url


class RollingAutoSensor(BaseSensorOperator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def poke(self, context):

        def get_index_of_alias(index_name: str):
            response = requests.get(f'{es_url}/_cat/aliases/{index_name}', verify=False)
            logging.info(f'Get alias for {index_name}:\n[{response.status_code}] {response.text}')
            if not response.ok:
                return None
            first_line = response.text.split('\n')[0]
            columns = first_line.split(' ')
            if len(columns) < 2:
                return None
            return columns[1]

        def get_color(index_name: str):
            if "blue" in index_name:
                return 'blue'
            if "green" in index_name:
                return 'green'
            return None

        def swap_aliases(color: str):
            url = f'{es_url}/_aliases'
            headers = {'Content-Type': 'application/json'}
            data = '''
            {{
                "actions": [
                    {{ "remove": {{ "index": "*", "alias": "clin-{env}-analyses" }} }},
                    {{ "remove": {{ "index": "*", "alias": "clin-{env}-sequencings" }} }},
                    {{ "add": {{ "index": "clin-{env}-analyses-{color}", "alias": "clin-{env}-analyses" }} }},
                    {{ "add": {{ "index": "clin-{env}-sequencings-{color}", "alias": "clin-{env}-sequencings" }} }}
                ]
            }}
            '''.format(
                env=env,
                color=color,
            )
            response = requests.post(url, verify=False, headers=headers, data=data)
            logging.info(f'Swap aliases for color {color}:\n[{response.status_code}] {response.text}')
            Slack.notify_dag_message(context, f'Detecting a FHIR reboot on color: {color}\nSwapping aliases: [{response.status_code}] {response.text}')
            if not response.ok:
                raise AirflowFailException(f'Failed to swap aliases')


        # detect the current QA state, working or not and the color
        # can use any index with an alias other than the ones FHIR uses
        # variant centric is kinda safe in that regard
        variant_centric_index = get_index_of_alias('clin_qa_variant_centric')

        if not variant_centric_index:
            logging.info('Variant centric not found')
            return False # QA is DOWN do nothing

        # find out if the analysis alias exist (wont be if FHIR did reboot)
        analysis_index = get_index_of_alias('clin-qa-analyses')

        if not analysis_index:
            color = get_color(variant_centric_index)
            if not color:
                logging.info('Color not found')
                return False

            # FHIR rebooted and we know the color
            logging.info('Analysis not found for color: {color}')
            
            # swap the aliases
            swap_aliases(color)
            
        return False # always restart the sensor,
