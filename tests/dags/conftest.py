import pytest
from airflow.models import DagBag

from tests.conftest import DAGS_DIR


@pytest.fixture(scope='session')
def dag_bag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
