from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from lib.config import K8sContext, basespace_illumina_credentials, config_file
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks.public_data import (PublicSourceDag, should_continue,
                                   update_public_data_info)
from lib.utils import http_get

spliceai_dag = PublicSourceDag(
    name='spliceai',
    display_name="SpliceAI",
    website="https://github.com/Illumina/SpliceAI"
)

with DAG(
        dag_id=spliceai_dag.dag_id,
        start_date=datetime(2022, 1, 1),
        schedule=None,
        params=PublicSourceDag.params,
        default_args=PublicSourceDag.default_args,
        max_active_tasks=1
) as dag:
    
    @task(task_id='file', on_execute_callback=Slack.notify_dag_start)
    def file():
        headers = {'x-access-token': f'{basespace_illumina_credentials}'}
        files = {
            "indel": [{"vcf": 16525003580 , "tbi": 16525276839}],
            "snv": [{"vcf": 16525380715, "tbi": 16525505189}]
        }

        for type, files in files.items():
            for file_data in files:
                file_name = f"spliceai_scores.raw.{type}.hg38.vcf.gz"

                # Get latest available version
                latest_ver = http_get(f'https://api.basespace.illumina.com/v1pre3/files/{file_data["vcf"]}', headers).json()['Response']['ETag']
                spliceai_dag.set_last_version(latest_ver, type)

                # Upload file index to S3 (if new)
                spliceai_dag.upload_file_if_new(
                    url=f'https://api.basespace.illumina.com/v1pre3/files/{file_data["tbi"]}/content',
                    file_name=f'{file_name}.tbi',
                    version_key=type,
                    save_version=False,
                    headers=headers)

                # Upload files to S3 (if new)
                spliceai_dag.upload_file_if_new(
                    url=f'https://api.basespace.illumina.com/v1pre3/files/{file_data["vcf"]}/content',
                    file_name=file_name,
                    version_key=type,
                    headers=headers,
                    stream=True)
                
        return spliceai_dag.serialize()

    
    dag_data = file()

    indel_table = SparkOperator(
        task_id='indel_table',
        name='etl-import-spliceai-indel-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'spliceai_indel',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_spliceai_indel_table',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    snv_table = SparkOperator(
        task_id='snv_table',
        name='etl-import-spliceai-snv-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'spliceai_snv',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_spliceai_snv_table',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    enrich_indel = SparkOperator(
        task_id='enrich_indel',
        name='etl-enrich-spliceai-indel',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-xlarge',
        arguments=[
            'spliceai_enriched_indel',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_enrich_spliceai_indel',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    enrich_snv = SparkOperator(
        task_id='enrich_snv',
        name='etl-enrich-spliceai-snv',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'spliceai_enriched_snv',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_enrich_spliceai_snv',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    dag_data >> should_continue(dag_data) >> [indel_table, snv_table]
    indel_table >> enrich_indel
    snv_table >> enrich_snv
    [enrich_snv, enrich_indel] >> update_public_data_info(dag_data)
