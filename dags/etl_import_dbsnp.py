import re
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, should_continue, update_public_data_info
from lib.utils import http_get, get_md5_from_url


dbsnp_dag = PublicSourceDag(
    name='dbsnp',
    display_name="NCBI dbSNP",
    website="https://www.ncbi.nlm.nih.gov/snp/",
    schedule='45 6 * * 6',  # every Saturday at 6:45am
)

with DAG(
    dag_id=dbsnp_dag.dag_id,
    start_date=datetime(2025, 8, 9),
    schedule=dbsnp_dag.schedule,
    params=dbsnp_dag.params,
    default_args=dbsnp_dag.default_args,
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)
    def file():
        url = 'https://ftp.ncbi.nih.gov/snp/latest_release/VCF'

        # Get latest version
        files = http_get(f'{url}').text
        dbsnp_dag.set_last_version(re.findall(r'GCF_000001405\.(\d+)\.gz\.md5', files)[-1])

        file_name = f'GCF_000001405.{dbsnp_dag.last_version}.gz'

        # Get MD5
        md5 = get_md5_from_url(f'{url}/{file_name}.md5')
        # Upload file to S3 (if new)
        tbiFile = f'{file_name}.tbi'
        dbsnp_dag.upload_file_if_new(url=f'{url}/{tbiFile}', file_name=tbiFile, save_version=False) # tbi file
        dbsnp_dag.upload_file_if_new(url=f'{url}/{file_name}', file_name=file_name, md5_hash=md5['hash'], stream=True)
    
        return dbsnp_dag

    dag_data = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-dbsnp-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'dbsnp',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_dbsnp_table',
        ]
    )

    dag_data >> should_continue(dag_data) >> table >> update_public_data_info(dag_data)
