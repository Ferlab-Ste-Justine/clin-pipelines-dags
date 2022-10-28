import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from datetime import datetime
from lib import config
from lib.config import env


if (config.show_test_dags):

    with DAG(
        dag_id='test_s3_success',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
    ) as dag:

        def _test_s3():
            s3 = S3Hook(config.s3_conn_id)
            s3_bucket = f'cqgc-{env}-app-datalake'
            list = s3.list_keys(s3_bucket)
            logging.info(list)

        test_s3 = PythonOperator(
            task_id='test_s3',
            python_callable=_test_s3,
        )
