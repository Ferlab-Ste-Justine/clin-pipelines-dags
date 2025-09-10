import logging
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import ShortCircuitOperator

IS_NEW_VERSION_KEY = 'is_new_version'

def skip_if_not_new_version(is_new_version, context):
    context['ti'].xcom_push(key=IS_NEW_VERSION_KEY, value=is_new_version)
    if not is_new_version:
        logging.info(f'The file is up to date!')
        raise AirflowSkipException()
    else:
        logging.info(f'The file has a new version!')
    return is_new_version

def _continue_if_not_new_version(**context) -> bool:
    return context["params"]["skip_if_not_new_version"] == 'no' or context['ti'].xcom_pull(key=IS_NEW_VERSION_KEY)

def should_continue():
    return ShortCircuitOperator(
        task_id='should_continue',
        python_callable=_continue_if_not_new_version
    )