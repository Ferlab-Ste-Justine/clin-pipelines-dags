import re
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import update_public_data_info, PublicSourceDag, should_continue
from lib.utils import get_md5_from_url


clinvar_dag = PublicSourceDag(
    name='clinvar',
    display_name="NCBI Gene",
    website="https://www.ncbi.nlm.nih.gov/clinvar/",
    schedule='0 6 * * 6',  # every Saturday at 6am
)

with DAG(
    dag_id= clinvar_dag.dag_id,
    start_date=datetime(2025, 8, 9),
    schedule=clinvar_dag.schedule,
    params=PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
    catchup=False
) as dag:
    
    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)
    def file():
        url = 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38'
        file_name = 'clinvar.vcf.gz'

        # Get MD5
        md5 = get_md5_from_url(f'{url}/{file_name}.md5')
        # Get latest version
        clinvar_dag.set_last_version(re.search('clinvar_([0-9]+)\.vcf', md5['text']).group(1))
        # Upload file to S3 (if new)
        clinvar_dag.upload_file_if_new(url=f'{url}/{file_name}', file_name=file_name, md5_hash=md5['hash'])

        return clinvar_dag.serialize()

    dag_data = file()

    table = SparkOperator(
        task_id='table',
        name='etl-import-clinvar-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'clinvar',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_clinvar_table',
        ]
    )

dag_data >> should_continue(dag_data) >> table >> update_public_data_info(dag_data)
