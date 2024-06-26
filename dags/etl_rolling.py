from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from lib.config import Env, K8sContext, env, es_url
from lib.operators.curl import CurlOperator
from lib.slack import Slack
from lib.tasks import arranger
from lib.tasks.params_validate import validate_release_color
from lib.utils_etl import color, release_id

if env == Env.QA:
    with DAG(
            dag_id='etl_rolling',
            start_date=datetime(2022, 1, 1),
            schedule_interval=None,
            params={
                'release_id': Param('', type='string'),
                'color': Param('', type=['null', 'string']),
            },
            default_args={
                'on_failure_callback': Slack.notify_task_failure,
                'trigger_rule': TriggerRule.NONE_FAILED,
            },
    ) as dag:
        params_validate = validate_release_color(release_id(), color())

        es_indices_swap = CurlOperator(
            task_id='es_indices_swap',
            name='etl-rolling-es-indices-swap',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                '-f', '-X', 'POST', f'{es_url}/_aliases',
                '-H', 'Content-Type: application/json', '-d',
                '''
                {{
                    "actions": [
                        {{ "remove": {{ "index": "*", "alias": "clin-{env}-analyses" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin-{env}-sequencings" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_gene_centric" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_gene_suggestions" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_variant_centric" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_variant_suggestions" }} }},
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_coverage_by_gene_centric" }} }},
                        {{ "add": {{ "index": "clin-{env}-analyses{dash_color}", "alias": "clin-{env}-analyses" }} }},
                        {{ "add": {{ "index": "clin-{env}-sequencings{dash_color}", "alias": "clin-{env}-sequencings" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_gene_centric_{release_id}", "alias": "clin_{env}_gene_centric" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_gene_suggestions_{release_id}", "alias": "clin_{env}_gene_suggestions" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_variant_centric_{release_id}", "alias": "clin_{env}_variant_centric" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_variant_suggestions_{release_id}", "alias": "clin_{env}_variant_suggestions" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_coverage_by_gene_centric_{release_id}", "alias": "clin_{env}_coverage_by_gene_centric" }} }}
                    ]
                }}
                '''.format(
                    env=env,
                    release_id=release_id(),
                    dash_color=color('-'),
                    under_color=color('_'),
                ),
            ],
        )

        es_cnv_centric_index_swap = CurlOperator(
            task_id='es_cnv_centric_index_swap',
            name='etl-rolling-es-cnv-centric-index-swap',
            k8s_context=K8sContext.DEFAULT,
            arguments=[
                '-f', '-X', 'POST', f'{es_url}/_aliases',
                '-H', 'Content-Type: application/json', '-d',
                '''
                {{
                    "actions": [
                        {{ "remove": {{ "index": "*", "alias": "clin_{env}_cnv_centric" }} }},
                        {{ "add": {{ "index": "clin_{env}{under_color}_cnv_centric_{release_id}", "alias": "clin_{env}_cnv_centric" }} }}
                    ]
                }}
                '''.format(
                    env=env,
                    release_id=release_id(),
                    dash_color=color('-'),
                    under_color=color('_'),
                ),
            ],
            skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
        )

        arranger_remove_project_task = arranger.remove_project()
        arranger_restart_task = arranger.restart(on_success_callback=Slack.notify_dag_completion)

        params_validate >> es_indices_swap >> es_cnv_centric_index_swap >> arranger_remove_project_task >> arranger_restart_task
