import http.client
import logging
import ssl
import time
import urllib.request
from pathlib import Path
from typing import List
from unittest.mock import patch

import airflow
import docker
import pytest
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from testcontainers.core.container import DockerContainer

DAGS_DIR = Path(__file__).parent.parent / 'dags'
RESOURCES_DIR = Path(__file__).parent / 'resources'

MOCK_VARIABLES = {
    'environment': 'test',
    's3_conn_id': 'clin_minio',
    's3_franklin': 'franklin_minio',
    's3_franklin_bucket': 'franklin',
    'franklin_url': 'https://mocks.qa.cqgc.hsj.rtss.qc.ca/api/franklin',
    'franklin_email': 'API_franklin_email',
    'franklin_password': 'test',
}

RUSTFS_IMAGE = "rustfs/rustfs:1.0.0"  # S3-compatible replacement for MinIO, whose images are no longer available
RUSTFS_ACCESS_KEY = "admin"
RUSTFS_SECRET_KEY = "password"
RUSTFS_API_PORT = 9000
RUSTFS_CONSOLE_PORT = 9001


class S3Instance:
    def __init__(self, host, api_port, console_port, access_key, secret_key):
        self.host = host
        self.api_port = api_port
        self.console_port = console_port
        self.endpoint = f"http://{host}:{api_port}"
        self.console_url = f"http://{host}:{console_port}"
        self.access_key = access_key
        self.secret_key = secret_key


@pytest.fixture(scope='session', autouse=True)
def mock_airflow_variables():
    def mock_get(key, default_var=None):
        return MOCK_VARIABLES.get(key, default_var)

    with patch('airflow.models.Variable.get', side_effect=mock_get):
        yield


def wait_for_s3_ready(endpoint: str, timeout: int = 30):
    # RustFS logs to a file rather than stdout, so poll its health endpoint instead of waiting for logs
    deadline = time.time() + timeout
    while True:
        try:
            with urllib.request.urlopen(f"{endpoint}/health", timeout=2) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        if time.time() > deadline:
            raise TimeoutError(f"S3 endpoint {endpoint} not ready after {timeout}s")
        time.sleep(0.5)


@pytest.fixture(scope="session")
def start_s3_container():
    containers = {}
    client = docker.from_env()

    def _start_s3_container(name):
        logging.info(f"Starting RustFS container with name: {name}")
        if name in containers:
            logging.info(f"Using existing RustFS instance for {name}")
            return containers[name]

        for container in client.containers.list():
            if name in container.name:
                logging.info(f"Found existing container with name: {name}")
                ports = container.attrs["NetworkSettings"]["Ports"]
                api_port = ports[f"{RUSTFS_API_PORT}/tcp"][0]["HostPort"]
                console_port = ports[f"{RUSTFS_CONSOLE_PORT}/tcp"][0]["HostPort"]
                instance = S3Instance("localhost", api_port, console_port, RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY)
                containers[name] = instance
                return instance  # No tear down, just return existing instance

        logging.info(f"Creating new RustFS container with name: {name}")
        container = (
            DockerContainer(RUSTFS_IMAGE)
            .with_name(name)
            .with_env("RUSTFS_ACCESS_KEY", RUSTFS_ACCESS_KEY)
            .with_env("RUSTFS_SECRET_KEY", RUSTFS_SECRET_KEY)
            .with_env("RUSTFS_CONSOLE_ENABLE", "true")
            .with_exposed_ports(RUSTFS_API_PORT, RUSTFS_CONSOLE_PORT)
        )
        container.start()

        api_port = container.get_exposed_port(RUSTFS_API_PORT)
        console_port = container.get_exposed_port(RUSTFS_CONSOLE_PORT)

        instance = S3Instance(container.get_container_host_ip(), api_port, console_port, RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY)
        wait_for_s3_ready(instance.endpoint)
        containers[name] = instance
        return instance

    yield _start_s3_container

    for name in containers:
        logging.info(f"Tearing down RustFS container with name: {name}")
        container = client.containers.get(name)
        container.remove(force=True)  # Remove, not just stop, so the name is free for the next run


def create_airflow_s3_connection(conn_id, s3_instance: S3Instance):
    session = airflow.settings.Session()

    # Overwrite existing connection if it exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        logging.info(f"Overwriting existing connection: {conn_id}")
        session.delete(existing_conn)
        session.commit()

    conn = Connection(
        conn_id=conn_id,
        conn_type="aws",
        extra={
            "endpoint_url": s3_instance.endpoint,
            "aws_access_key_id": s3_instance.access_key,
            "aws_secret_access_key": s3_instance.secret_key,
            "region_name": "us-east-1",
            "addressing_style": "path"
        }
    )
    session.add(conn)
    session.commit()


@pytest.fixture(scope="session")
def get_s3_hook(start_s3_container):
    def _start_container_and_create_conn(name):
        s3_instance = start_s3_container(name)
        create_airflow_s3_connection(name, s3_instance)
        return S3Hook(aws_conn_id=name)

    return _start_container_and_create_conn


@pytest.fixture(scope="session")
def clin_minio(get_s3_hook):
    from lib.config import all_qlin_buckets, s3_conn_id
    s3_hook = get_s3_hook(s3_conn_id)

    for bucket in all_qlin_buckets:
        if not s3_hook.check_for_bucket(bucket):
            logging.info(f"Creating bucket: {bucket}")
            s3_hook.create_bucket(bucket_name=bucket)

    yield s3_hook


@pytest.fixture(scope="function")
def clean_up_clin_minio(clin_minio):
    from lib.config import all_qlin_buckets

    def _clean_up():
        for bucket in all_qlin_buckets:
            keys = clin_minio.list_keys(bucket_name=bucket)
            if keys:
                logging.info(f"Emptying bucket: {bucket}")
                clin_minio.delete_objects(bucket=bucket, keys=keys)

    _clean_up()  # Clean up before test
    yield
    _clean_up()  # Clean up after test


@pytest.fixture(scope="function")
def clin_minio_with_clean_up(clin_minio, get_s3_hook):
    from lib.config import all_qlin_buckets

    s3_hook = clin_minio

    for bucket in all_qlin_buckets:
        keys = clin_minio.list_keys(bucket_name=bucket)
        if keys:
            logging.info(f"Emptying bucket: {bucket}")
            clin_minio.delete_objects(bucket=bucket, keys=keys)

    yield s3_hook


@pytest.fixture(scope="session")
def franklin_s3(get_s3_hook):
    from lib.config import s3_franklin, s3_franklin_bucket
    s3_hook = get_s3_hook(s3_franklin)

    if not s3_hook.check_for_bucket(s3_franklin_bucket):
        logging.info(f"Creating Franklin bucket: {s3_franklin_bucket}")
        s3_hook.create_bucket(bucket_name=s3_franklin_bucket)

    yield s3_hook


@pytest.fixture(scope="session", autouse=True)
def disable_get_franklin_http_conn_ssl(mock_airflow_variables):
    from lib.franklin import franklin_url_parts

    def patched_get_franklin_http_conn():
        # Inject the unverified SSL context
        conn = http.client.HTTPSConnection(franklin_url_parts.hostname, context=ssl._create_unverified_context())
        return conn

    with patch('lib.franklin.get_franklin_http_conn', new=patched_get_franklin_http_conn):
        yield


@pytest.fixture
def load_vcfs():
    def _load_vcfs(s3: S3Hook, bucket: str, paths: List[str]):
        for path in paths:
            s3.load_string(f'VCF content for {path}', key=path, bucket_name=bucket, replace=True)

    return _load_vcfs


@pytest.fixture
def solo_analysis_row() -> dict:
    """
    Solo analysis submitted using legacy method (batch_id provided).
    """
    return {'analysis_id': 'A1', 'family_id': None, 'aliquot_id': '1', 'sequencing_id': 'S1', 'batch_id': 'BATCH_1',
            'is_proband': True, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'affected_status': True,
            'first_name': 'Jean', 'birth_date': '2000-01-01', 'gender': 'Male', 'clinical_signs': None}


@pytest.fixture
def trio_analysis_rows() -> List[dict]:
    """
    Trio analysis submitted using new method (dummy batch_id).
    """
    return [
        {'analysis_id': 'A2', 'family_id': 'FM1', 'aliquot_id': '2', 'sequencing_id': 'S2', 'batch_id': 'foobar',
         'is_proband': True, 'father_aliquot_id': '3', 'mother_aliquot_id': '4', 'affected_status': True,
         'first_name': 'Jean', 'birth_date': '2000-01-01', 'gender': 'Male', 'clinical_signs': [
            {'id': 'HP:0000001'}, {'id': 'HP:0000002'}
        ]},
        {'analysis_id': 'A2', 'family_id': 'FM1', 'aliquot_id': '3', 'sequencing_id': 'S3', 'batch_id': 'foobar',
         'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'affected_status': False,
         'first_name': 'Jeannot', 'birth_date': '1970-01-01', 'gender': 'Male', 'clinical_signs': None},
        {'analysis_id': 'A2', 'family_id': 'FM1', 'aliquot_id': '4', 'sequencing_id': 'S4', 'batch_id': 'foobar',
         'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'affected_status': True,
         'first_name': 'Jeanne', 'birth_date': '1975-01-01', 'gender': 'Female', 'clinical_signs': None}
    ]


@pytest.fixture
def duo_analysis_rows() -> List[dict]:
    """
    Duo analysis submitted using new method (dummy batch_id).
    """
    return [
        {'analysis_id': 'A3', 'family_id': 'FM2', 'aliquot_id': '5', 'sequencing_id': 'S5', 'batch_id': 'foobar',
         'is_proband': True, 'father_aliquot_id': None, 'mother_aliquot_id': 6, 'affected_status': True},
        {'analysis_id': 'A3', 'family_id': 'FM2', 'aliquot_id': '6', 'sequencing_id': 'S6', 'batch_id': 'foobar',
         'is_proband': False, 'father_aliquot_id': None, 'mother_aliquot_id': None, 'affected_status': False}
    ]


@pytest.fixture
def clinical_data(solo_analysis_row, trio_analysis_rows, duo_analysis_rows) -> List[dict]:
    return [
        solo_analysis_row,
        *trio_analysis_rows,
        *duo_analysis_rows
    ]