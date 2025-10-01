import base64
import re
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from lib import config
from lib.config import K8sContext, config_file
from lib.operators.spark import SparkOperator
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.slack import Slack
from lib.tasks.public_data import PublicSourceDag, update_public_data_info, should_continue
from lib.utils import http_get


cosmic_dag = PublicSourceDag(
    name='cosmic',
    display_name="COSMIC",
    website="https://www.cosmickb.org/",
)

with DAG(
        dag_id=cosmic_dag.dag_id,
        start_date=datetime(2022, 1, 1),
        schedule=None,
        params=PublicSourceDag.params,
        default_args=PublicSourceDag.default_args,
        catchup=False,
        max_active_runs=1
) as dag:

    @task(task_id='files', on_execute_callback=Slack.notify_dag_start)
    def files():
        url = 'https://cancer.sanger.ac.uk/cosmic'
        path = 'file_download/GRCh38/cosmic'

        # TODO: Download Cosmic_CancerGeneCensus_GRCh38.tar when scripted downloads are added to new download page
        # gene_census_file = 'cancer_gene_census.csv'
        mutation_census_file = 'cmc_export.tsv.gz'
        mutation_census_archive = 'CMC.tar'

        # Get latest version
        # TODO: Cosmic need an account to access the download page: https://ferlab-crsj.atlassian.net/browse/CLIN-4440
        cosmic_dag.last_version = cosmic_dag.get_last_version_from_url(url, 'COSMIC (v[0-9]+),')

        for file_name in [mutation_census_file]:
            # Encode credentials
            headers = {'Authorization': f'Basic {base64.b64encode(config.cosmic_credentials.encode()).decode()}'}

            # Get file url
            download_url = http_get(
                f'{url}/{path}/{cosmic_dag.last_version}/{mutation_census_archive if file_name == mutation_census_file else file_name}',
                headers=headers
            ).json()['url']
            
            # Upload file to S3 (if new)
            cosmic_dag.upload_file_if_new(download_url, file_name, headers=headers, tar_extract=mutation_census_file if file_name == mutation_census_file else None)

        return cosmic_dag.serialize()

    dag_data = files()

    gene_table = SparkOperator(
        task_id='gene_table',
        name='etl-import-cosmic-gene-set-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'cosmic_gene_set',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_cosmic_gene_set_table',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    mutation_table = SparkOperator(
        task_id='mutation_table',
        name='etl-import-cosmic-mutation-set-table',
        k8s_context=K8sContext.ETL,
        spark_class='bio.ferlab.datalake.spark3.publictables.ImportPublicTable',
        spark_config='config-etl-large',
        arguments=[
            'cosmic_mutation_set',
            '--config', config_file,
            '--steps', 'default',
            '--app-name', 'etl_import_cosmic_mutation_set_table',
        ],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    trigger_genes = TriggerDagRunOperator(
        task_id='genes',
        trigger_dag_id='etl_import_genes',
        wait_for_completion=False,
    )

    dag_data >> should_continue(dag_data) >> [gene_table, mutation_table] >> trigger_genes >> update_public_data_info(dag_data)
