from airflow import DAG
from datetime import datetime
from lib.config import env, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, update_public_data_info
from datetime import datetime

from airflow import DAG

from lib.config import env, K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack

dbnsfp_dag = PublicSourceDag(
    name='dbNSFP',
    website="https://www.dbnsfp.org/"
)

with DAG(
    dag_id=dbnsfp_dag.dag_id,
    start_date=datetime(2022, 1, 1),
    schedule=None,
    params=PublicSourceDag.params,
    default_args=PublicSourceDag.default_args,
) as dag:

    # Following steps explain how to get and clean-up the raw zip files
    # 1 - download dbNSFP4.3a.zip from https://sites.google.com/site/jpopgen/dbNSFP
    # 2 - extract ZIP content on local inside a folder
    # 3 - keep only files with pattern: dbNSFP4.3a_variant.chr*.gz
    # 4 - for every of those files, apply the following shell script:
    # (this script exists because: some columns have un-compatible names for HIVE (SQL like naming convention) and the easiest way was a 'sed' 
    # command cause the files are big also the script deploy on S3 QA when it's done, can take some time with the VPN upload speed limit. 
    # You can run multiple conversions at once to go quicker, one per local CPU unit is fine)

    '''
    gunzip $1.gz
    sed -i -e '1s/pos(1-based)/position_1-based/' -e '1s/hg19_pos(1-based)/hg19_pos_1-based/' -e '1s/hg18_pos(1-based)/phg18_pos_1-based/' $1
    gzip $1
    aws --profile cqgc-qa --endpoint https://s3.cqgc.hsj.rtss.qc.ca s3 cp $1.gz s3://cqgc-qa-app-datalake/raw/landing/dbNSFP/$1.gz
    '''

    dag_data = dbnsfp_dag.serialize()

    raw = SparkOperator(
        task_id='raw',
        name='etl-import-dbnsfp-raw-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[f'config/{env}.conf', 'default', 'dbnsfp_raw'],
        on_execute_callback=Slack.notify_dag_start,
    )

    enriched = SparkOperator(
        task_id='enriched',
        name='etl-import-dbnsfp-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'dbnsfp',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_dbnsfp_table',
        ],
        on_success_callback=Slack.notify_dag_completion,
    )

    raw >> enriched >> update_public_data_info(dag_data)
