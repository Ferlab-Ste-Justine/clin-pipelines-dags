from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from lib import config
from lib.config import K8sContext, env
from lib.operators.spark import SparkOperator

if (config.show_test_dags):

    with DAG(
        dag_id='test_spark_fail',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        params={
            'spark_jar': Param('', type=['null', 'string']),
        },
    ) as dag:

        def spark_jar() -> str:
            return '{{ params.spark_jar or "" }}'
            
        test_spark_fail = SparkOperator(
            task_id='test_spark_fail',
            name='test-spark-fail',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.fail.Fail',
            spark_config='config-etl-large',
            spark_jar=spark_jar(),
            arguments=[f'config/{env}.conf', 'default'],
        )
