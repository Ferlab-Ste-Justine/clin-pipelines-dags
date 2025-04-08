from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.config import K8sContext, config_file
from lib.operators.panels import PanelsOperator
from lib.operators.spark import SparkOperator
from lib.slack import Slack
from lib.tasks import (enrich, es, index, params_validate, prepare_index,
                       publish_index)
from lib.utils_etl import color, spark_jar

with DAG(
    dag_id='etl_import_panels',
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
        params={
        'panels': Param('', type='string'),
        'color': Param('', type=['null', 'string']),
        'debug': Param('no', enum=['yes', 'no']),
        'dryrun': Param('no', enum=['yes', 'no']),
        'import_and_normalize': Param('yes', enum=['yes', 'no']),
        'enrich_and_index': Param('no', enum=['yes', 'no']),
        'spark_jar': Param('', type=['null', 'string']),
    },
    default_args={
        'trigger_rule': TriggerRule.NONE_FAILED,
        'on_failure_callback': Slack.notify_task_failure,
    },
    max_active_tasks=1,
    max_active_runs=1
) as dag:

    def panels() -> str:
        return '{{ params.panels }}'

    def _import() -> str:
        return '{{ params.import }}'

    def debug() -> str:
        return '{% if params.debug == "yes" %}true{% else %}false{% endif %}'

    def dryrun() -> str:
        return '{% if params.dryrun == "yes" %}true{% else %}false{% endif %}'

    def skip_import() -> str:
        return '{% if params.panels|length and params.import_and_normalize == "yes" %}{% else %}yes{% endif %}'

    def skip_normalize() -> str:
        return '{% if params.dryrun == "yes" %}yes{% else %}{% endif %}'
    
    def skip_enrich() -> str:
        return '{% if params.dryrun == "yes" or params.enrich_and_index == "no" %}yes{% else %}{% endif %}'
    
    params_validate_color = params_validate.validate_color(color())

    @task_group(group_id='import_and_normalize')
    def import_and_normalize():


        @task(task_id='params_validate_import')
        def params_validate_import(panels, _import):
            if panels == '' and _import == 'yes':
                raise AirflowFailException('DAG param "panels" is required')

        import_panels_s3 = PanelsOperator(
            task_id='import_panels_s3',
            name='etl-s3-panels',
            k8s_context=K8sContext.DEFAULT,
            skip=skip_import(),
            arguments=[
                'org.clin.panels.command.Import', '--file=' + panels(), '--debug=' + debug(), '--dryrun=' + dryrun()
            ],
        )

        normalize_panels = SparkOperator(
            task_id='normalize_panels',
            name='etl-import-panels',
            k8s_context=K8sContext.ETL,
            spark_class='bio.ferlab.clin.etl.normalized.RunNormalized',
            spark_config='config-etl-large',
            skip=skip_normalize(),
            arguments=[
                'panels',
                '--config', config_file,
                '--steps', 'initial',
                '--app-name', 'etl_import_panels',
            ],
        )

        params_validate_import(panels(), _import()) >> import_panels_s3 >> normalize_panels

    @task_group(group_id='enrich_and_index')
    def enrich_and_index():
        enrich_variants = enrich.variants(spark_jar=spark_jar(), skip=skip_enrich(), task_id='enrich_variants')
        prepare_variants = prepare_index.variant_centric(spark_jar=spark_jar, skip=skip_enrich(), task_id='prepare_variants')
        release_id = es.get_release_id(release_id=None, color=color('_'), index='variant_centric', skip=skip_enrich())
        index_variants = index.variant_centric(release_id, color('_'), spark_jar(), task_id='index_variant_centric', skip=skip_enrich())
        publish_variants = publish_index.variant_centric(release_id, color('_'), spark_jar(), task_id='publish_variant_centric', skip=skip_enrich())
        delete_previous_release = es.delete_previous_release('variant_centric', release_id, color('_'), skip=skip_enrich())

        enrich_variants >> prepare_variants >> release_id >> index_variants >> publish_variants >> delete_previous_release

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    params_validate_color >> import_and_normalize() >> enrich_and_index() >> slack