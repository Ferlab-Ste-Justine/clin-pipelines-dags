from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
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
        max_active_tasks=4
) as dag:
    
    headers = {'x-access-token': f'{basespace_illumina_credentials}'}
    files_by_type = {
                "indel": [{"vcf": 16525003580 , "tbi": 16525276839}],
                "snv": [{"vcf": 16525380715, "tbi": 16525505189}]
            }
    
    @task(task_id='get_version', on_execute_callback=Slack.notify_dag_start)
    def get_version():
        for type, files in files_by_type.items():
            for file_data in files:
                # Get latest available version
                latest_ver = http_get(f'https://api.basespace.illumina.com/v1pre3/files/{file_data["vcf"]}', headers).json()['Response']['ETag']
                spliceai_dag.set_last_version(latest_ver, type)
        return spliceai_dag

    dag_data = get_version()

    @task_group(group_id='download_files')
    def download_files(dag_data: PublicSourceDag):    

        @task(task_id='download_file', max_active_tis_per_dag=2)
        def download_file(dag_data: PublicSourceDag, type: str, file_data: dict):
            file_name = f"spliceai_scores.raw.{type}.hg38.vcf.gz"

            # Upload file index to S3 (if new)
            dag_data.upload_file_if_new(
                url=f'https://api.basespace.illumina.com/v1pre3/files/{file_data["tbi"]}/content',
                file_name=f'{file_name}.tbi',
                version_key=type,
                save_version=False,
                headers=headers)

            # Upload files to S3 (if new)
            dag_data.upload_file_if_new(
                url=f'https://api.basespace.illumina.com/v1pre3/files/{file_data["vcf"]}/content',
                file_name=file_name,
                version_key=type,
                headers=headers,
                stream=True)
                    
            return spliceai_dag

        [
            download_file.override(task_id='download_indel_files').partial(dag_data=dag_data, type="indel").expand(file_data=files_by_type["indel"]),
            download_file.override(task_id='download_snv_files').partial(dag_data=dag_data, type="snv").expand(file_data=files_by_type["snv"])
        ]

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

    dag_data >> should_continue(dag_data) >> download_files(dag_data) >> indel_table >> snv_table >> enrich_indel >> enrich_snv >> update_public_data_info(dag_data)
