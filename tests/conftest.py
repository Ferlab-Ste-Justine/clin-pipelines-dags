import http.client
import ssl
from pathlib import Path
from unittest.mock import patch

import airflow
import docker
import pytest
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

DAGS_DIR = Path(__file__).parent.parent / 'dags'
MOCK_VARIABLES = {
    'environment': 'test',
    's3_conn_id': 'clin_minio',
    's3_franklin': 'franklin_minio',
    's3_franklin_bucket': 'franklin',
    'franklin_url': 'https://mocks.qa.cqgc.hsj.rtss.qc.ca/api/franklin',
    'franklin_email': 'API_franklin_email',
    'franklin_password': 'test',
}

MINIO_IMAGE = "minio/minio:latest"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_API_PORT = 9000
MINIO_CONSOLE_PORT = 9001


class MinioInstance:
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


@pytest.fixture(scope="session")
def start_minio_container():
    containers = {}
    client = docker.from_env()

    def _start_minio_container(name):
        print(f"Starting MinIO container with name: {name}")
        if name in containers:
            print(f"Using existing MinIO instance for {name}")
            return containers[name]

        for container in client.containers.list():
            if name in container.name:
                print(f"Found existing container with name: {name}")
                ports = container.attrs["NetworkSettings"]["Ports"]
                api_port = ports[f"{MINIO_API_PORT}/tcp"][0]["HostPort"]
                console_port = ports[f"{MINIO_CONSOLE_PORT}/tcp"][0]["HostPort"]
                instance = MinioInstance("localhost", api_port, console_port, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
                containers[name] = instance
                return instance  # No tear down, just return existing instance

        print(f"Creating new MinIO container with name: {name}")
        container = (
            DockerContainer(MINIO_IMAGE)
            .with_name(name)
            .with_env("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
            .with_env("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
            .with_exposed_ports(MINIO_API_PORT, MINIO_CONSOLE_PORT)
            .with_command("server /data --console-address ':9001'")
        )
        container.start()
        wait_for_logs(container, "API:", timeout=30)

        api_port = container.get_exposed_port(MINIO_API_PORT)
        console_port = container.get_exposed_port(MINIO_CONSOLE_PORT)

        instance = MinioInstance("localhost", api_port, console_port, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
        containers[name] = instance
        return instance

    yield _start_minio_container

    for name in containers:
        print(f"Tearing down MinIO container with name: {name}")
        container = client.containers.get(name)
        container.stop()


def create_airflow_s3_connection(conn_id, minio_instance: MinioInstance):
    session = airflow.settings.Session()

    # Overwrite existing connection if it exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        print(f"Overwriting existing connection: {conn_id}")
        session.delete(existing_conn)
        session.commit()

    conn = Connection(
        conn_id=conn_id,
        conn_type="aws",
        extra={
            "endpoint_url": minio_instance.endpoint,
            "aws_access_key_id": minio_instance.access_key,
            "aws_secret_access_key": minio_instance.secret_key,
            "region_name": "us-east-1",
            "addressing_style": "path"
        }
    )
    session.add(conn)
    session.commit()


@pytest.fixture(scope="session")
def get_s3_hook(start_minio_container):
    def _start_container_and_create_conn(name):
        minio_instance = start_minio_container(name)
        create_airflow_s3_connection(name, minio_instance)
        return S3Hook(aws_conn_id=name)

    return _start_container_and_create_conn


@pytest.fixture(scope="session")
def clin_minio(get_s3_hook):
    from lib.config import all_qlin_buckets, s3_conn_id
    s3_hook = get_s3_hook(s3_conn_id)

    for bucket in all_qlin_buckets:
        if not s3_hook.check_for_bucket(bucket):
            print(f"Creating bucket: {bucket}")
            s3_hook.create_bucket(bucket_name=bucket)

    yield s3_hook


@pytest.fixture(scope="session")
def franklin_s3(get_s3_hook):
    from lib.config import s3_franklin, s3_franklin_bucket
    s3_hook = get_s3_hook(s3_franklin)

    if not s3_hook.check_for_bucket(s3_franklin_bucket):
        print(f"Creating Franklin bucket: {s3_franklin_bucket}")
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
