import json
import logging
import re
import ast
import sys
import tarfile
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, ClassVar
from airflow.decorators import task
from airflow.models import DagRun
from airflow.models.param import Param
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.config import env
from lib.slack import Slack
from lib.utils import http_get
from lib.utils_s3 import get_s3_file_version, http_get_file, stream_upload_or_resume_to_s3, file_md5
from multiprocessing import Lock


s3 = S3Hook(config.s3_conn_id)
s3_public_bucket = f'cqgc-{env}-app-public'
s3_public_data_file_key = 'public-databases.json'

lock = Lock()

@dataclass
class PublicSourceInfo:
    dag_id: str
    source: int
    url: str
    version: str = ""
    lastUpdate: str = ""
    frequency: str = None


def _init_last_update(public_source_info: PublicSourceInfo):
    dag_successful_runs = DagRun.find(dag_id=public_source_info.dag_id, state=State.SUCCESS)
    runs_dates = list(map(lambda dr: dr.end_date.isoformat(), dag_successful_runs))
    if not runs_dates:
        logging.warning(f"no successful run found for PublicSource entry '{public_source_info.dag_id}'")
    else:
        logging.info(f"last successful runs found'{runs_dates}'")
        runs_dates.reverse()
        public_source_info.lastUpdate = runs_dates[0]

    return public_source_info


def _public_sources_to_json(public_sources_info: list[PublicSourceInfo]) -> str:
    return json.dumps([asdict(item) for item in public_sources_info], sort_keys=True, default=str)


def _json_to_public_sources(json_string: str) -> list[PublicSourceInfo]:
    return [PublicSourceInfo(**item) for item in json.loads(json_string)]


def _get_public_data_json() -> list[PublicSourceInfo]:
    # Check if the file exists
    if not s3.check_for_key(s3_public_data_file_key, s3_public_bucket):
        s3.load_string(_public_sources_to_json([]), s3_public_data_file_key, s3_public_bucket, replace=True)
    # Read and return the file content as json
    return _json_to_public_sources(s3.read_key(s3_public_data_file_key, s3_public_bucket))


class PublicSourceDag:
    __version__: ClassVar[int] = 1
    params={
        'skip_if_not_new_version': Param('yes', enum=['yes', 'no']),
    }
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    }

    def __init__(self, name: str,
                 s3_bucket: str = f'cqgc-{env}-app-datalake',
                 raw_folder: str = None,
                 s3_key: str = None,
                 display_name: str = None,
                 website: str = None,
                 schedule: str = None,
                 last_version: str = None,
                 is_new_version: bool = False,
                 add_to_file: bool = True):
        if not name:
            raise Exception("'name' must be provided")
        self.name = name
        self.dag_id = f'etl_import_{name.lower()}'
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key if s3_key else f'raw/landing/{raw_folder if raw_folder else name}'
        self.display_name = display_name or name
        self.website = website
        self.schedule = get_schedule_by_env(schedule)
        self.last_version = last_version
        self.is_new_version = is_new_version
        if add_to_file and "pytest" not in sys.modules: # Disable this section for tests (s3 config does not exist)
            self._addpublic_source_to_data_file()


    def serialize(self) -> dict[str, Any]:
        return {
            'name': self.name,
            's3_bucket': self.s3_bucket,
            's3_key': self.s3_key,
            'display_name': self.display_name,
            'website': self.website,
            'last_version': self.last_version,
            'is_new_version': self.is_new_version,
        }
    

    @staticmethod
    def deserialize(data: dict[str, Any], version: int):
        return PublicSourceDag(add_to_file = False, **data)


    def get_info(self) -> PublicSourceInfo:
        return PublicSourceInfo(
            dag_id= self.dag_id,
            source= self.display_name,
            url= self.website,
            version= self.get_current_version(),
            frequency= self.schedule
        )
    

    def _addpublic_source_to_data_file(self):
        source_info = _init_last_update(self.get_info())
        public_sources = _get_public_data_json()
        exist = False
        for entry in public_sources:
            if entry.dag_id == self.dag_id:
                entry.source = source_info.source
                entry.url = source_info.url
                entry.frequency = source_info.frequency
                exist = True
                break

        # The lock is automatically acquired and released when the with block is exited
        with lock:
            if not exist:
                public_sources.append(source_info)
            s3.load_string(_public_sources_to_json(public_sources), s3_public_data_file_key, s3_public_bucket, replace=True)
            logging.info(f"PublicSource entry '{self.dag_id}' added to '{s3_public_data_file_key}'")


    def get_current_version(self, version_key: str = None) -> str:
        version = get_s3_file_version(s3, self.s3_bucket, f'{self.s3_key}/{self.name}')
        if version and version_key:
            return ast.literal_eval(version)[version_key]
        return version


    def set_last_version(self, version: str, version_key: str = None):
        if version_key:
            current_version = self.get_current_version()
            version_dict = ast.literal_eval(current_version) if current_version else {}
            version_dict[version_key] = version
            self.last_version = str(version_dict)
        else:
            self.last_version = version


    def set_last_version_from_url(self, url: str, regex, version_key: str = None) -> str:
        self.set_last_version(re.search(regex, http_get(url).text).group(1), version_key)


    def check_is_new_version(self) -> bool:
        current_version = self.get_current_version()
        logging.info(f'current version: {current_version}')
        logging.info(f'last version: {self.last_version}')
        self.is_new_version = current_version != self.last_version
        logging.info(f'The file has a new version!' if self.is_new_version else f'The file is up to date!')
        return self.is_new_version
    

    def save_version(self, version_key: str = None):
        if self.last_version:
            if version_key:
                current_version = self.get_current_version()
                version_dict = ast.literal_eval(current_version) if current_version else {}
                version_dict[version_key] = self.last_version
                self.last_version = str(version_dict)
            
            s3.load_string(self.last_version, f'{self.s3_key}/{self.name}.version', self.s3_bucket, replace=True)


    def save_file(self, file: str, file_key: str = None, check_version = True, save_version = True, version_key: str = None, save_md5 = False):
        if check_version and not self.check_is_new_version():
            return
        
        file_name = file_key if file_key else file
        s3.load_file(file, f'{self.s3_key}/{file_name}', self.s3_bucket, replace=True)
        logging.info(f'file ({file_name}) successfully saved.')

        if save_version:
            self.save_version(version_key)

        if save_md5:
            md5 = file_md5(file)
            s3.load_string(md5, f'{self.s3_key}/{file_name}.md5', self.s3_bucket, replace=True)


    def upload_file_if_new(self,
                           url,
                           file_name: str,
                           md5_hash: str = None,
                           headers = None,
                           stream = False,
                           tar_extract = False,
                           save_version = True,
                           version_key: str = None):
        if not self.check_is_new_version():
            return

        if stream:
            if tar_extract:
                raise Exception("tar_extract is not supported when streaming file")
            stream_upload_or_resume_to_s3(s3, self.s3_bucket, f'{self.s3_key}/{file_name}', url, headers=headers, md5=md5_hash)
            if save_version:
                self.save_version(version_key)
        else:
            logging.info(f'Start upload of {url}')
            http_get_file(f'{url}', file_name, headers=headers, md5=md5_hash)
            if tar_extract:
                with tarfile.open(file_name, 'r') as tar:
                    tar.extract(tar_extract)

            self.save_file(file_name, f'{tar_extract if tar_extract else file_name}', check_version=False, save_version=save_version)


@task(task_id='update_public_data_info', trigger_rule=TriggerRule.NONE_FAILED, on_success_callback=Slack.notify_dag_completion)
def update_public_data_info(dag_data: PublicSourceDag, **context):
    # The lock is automatically acquired and released when the with block is exited
    with lock:
        dag_id = context['dag'].dag_id
        version = dag_data.last_version
        logging.info(f"Updating public data info for dag '{dag_id}' with version '{version}'")
        # Check if the entry already exists
        public_sources = _get_public_data_json()
        for entry in public_sources:
            if entry.dag_id == dag_id:
                entry.lastUpdate = datetime.now().isoformat()
                entry.version = version if version else entry.version
                entry.frequency = context['dag'].schedule_interval if context['dag'].schedule_interval else ""
                break
        else:
            raise Exception(f"PublicSource entry '{dag_id}' not found")

        # Save to S3
        s3.load_string(_public_sources_to_json(public_sources), s3_public_data_file_key, s3_public_bucket, replace=True)


def should_continue(dag_data: PublicSourceDag):
    def check_is_new_version(dag_data: PublicSourceDag, **context):
        dont_skip = context["params"]["skip_if_not_new_version"] == 'no'
        is_new_version = dag_data.check_is_new_version()
        if not is_new_version and dont_skip:
            logging.info("skip_if_not_new_version is set to 'no', continuing the DAG even if the version is not new")
        return dont_skip or is_new_version

    return ShortCircuitOperator(
        task_id='should_continue',
        python_callable=check_is_new_version,
        op_kwargs={'dag_data': dag_data}
    )


def get_schedule_by_env(schedule: str):
    if env == 'prod' or not schedule:
        return schedule
    splits = schedule.split(' ')
    hour = int(splits[1])
    if env == 'qa':
        return f'{splits[0]} {hour + 4} {splits[2]} {splits[3]} {splits[4]}'
    if env == 'staging':
        return f'{splits[0]} {hour + 8} {splits[2]} {splits[3]} {splits[4]}'
    return schedule
