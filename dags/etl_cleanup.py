from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from lib.config import env, es_url, Env, K8sContext
from lib.operators.aws import AwsOperator
from lib.operators.curl import CurlOperator
from lib.operators.k8s_deployment_pause import K8sDeploymentPauseOperator
from lib.operators.k8s_deployment_restart import K8sDeploymentRestartOperator
from lib.operators.k8s_deployment_resume import K8sDeploymentResumeOperator
from lib.operators.postgres import PostgresOperator
from lib.operators.wait import WaitOperator
from lib.slack import Slack
from lib.tasks.params_validate import validate_color
from lib.utils_etl import color

if env in [Env.QA, Env.STAGING]:

    with DAG(
        dag_id='etl_cleanup',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'color': Param('', type=['null', 'string']),
        },
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
    ) as dag:

        params_validate = validate_color(color())

        fhir_pause = K8sDeploymentPauseOperator(
            task_id='fhir_pause',
            k8s_context=K8sContext.DEFAULT,
            deployment='fhir-server' + color('-'),
        )

        db_tables_delete = PostgresOperator(
            task_id='db_tables_delete',
            name='etl-cleanup-db-tables-delete',
            k8s_context=K8sContext.DEFAULT,
            color=color(),
            cmds=[
                'psql', '-d', 'fhir' + color('_'), '-c',
                '''
                DO $$$DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END$$$;
                ''',
            ],
        )

        fhir_resume = K8sDeploymentResumeOperator(
            task_id='fhir_resume',
            k8s_context=K8sContext.DEFAULT,
            deployment='fhir-server' + color('-'),
        )

        fhir_restart = K8sDeploymentRestartOperator(
            task_id='fhir_restart',
            k8s_context=K8sContext.DEFAULT,
            deployment='fhir-server' + color('-'),
        )

        es_indices_delete = CurlOperator(
            task_id='es_indices_delete',
            name='etl-cleanup-es-indices-delete',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                '--cacert', '/opt/ingress-ca/ca.crt', '-f', '-X', 'DELETE',
                f'{es_url}/clin-{env}-prescriptions' + color('-') +
                f',clin-{env}-patients' + color('-') +
                f',clin-qa-analyses' + color('-') +
                f',clin-qa-sequencings' + color('-') +
                f',clin-staging-analyses*' +
                f',clin-staging-sequencings*' +
                '?ignore_unavailable=true',
            ],
        )

        s3_download_delete = AwsOperator(
            task_id='s3_download_delete',
            name='etl-cleanup-s3-download-delete',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                's3', '--endpoint-url', 'https://s3.cqgc.hsj.rtss.qc.ca', 'rm',
                f's3://cqgc-{env}-app-download' + color('/') + '/',
                '--recursive',
            ],
        )

        s3_datalake_delete = AwsOperator(
            task_id='s3_datalake_delete',
            name='etl-cleanup-s3-datalake-delete',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                's3', '--endpoint-url', 'https://s3.cqgc.hsj.rtss.qc.ca', 'rm',
                f's3://cqgc-{env}-app-datalake/', '--recursive', '--exclude', '*',
                '--include', 'normalized/*',
                '--include', 'enriched/*',
                '--include', 'raw/landing/fhir/*',
                '--include', 'es_index/*',
            ],
        )

        wait_2m = WaitOperator(
            task_id='wait_2m',
            time='2m',
            on_success_callback=Slack.notify_dag_completion,
        )

        params_validate >> fhir_pause >> db_tables_delete >> fhir_resume >> fhir_restart >> es_indices_delete >> s3_download_delete >> s3_datalake_delete >> wait_2m
