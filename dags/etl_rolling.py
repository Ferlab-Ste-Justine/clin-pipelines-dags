from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

from lib.config import Env, K8sContext, env, es_url
from lib.doc import rolling as doc
from lib.groups.index.get_release_ids import get_release_ids
from lib.operators.curl import CurlOperator
from lib.slack import Slack
from lib.tasks import arranger
from lib.tasks.params_validate import validate_color
from lib.utils_etl import color, release_id

if env == Env.QA:
    with DAG(
            dag_id='etl_rolling',
            start_date=datetime(2022, 1, 1),
            schedule=None,
            doc_md=doc.rolling,
            params={
                'release_id': Param('', type=['null', 'string']),
                'gene_centric_release_id': Param('', type=['null', 'string']),
                'gene_suggestions_release_id': Param('', type=['null', 'string']),
                'variant_centric_release_id': Param('', type=['null', 'string']),
                'variant_suggestions_release_id': Param('', type=['null', 'string']),
                'coverage_by_gene_centric_release_id': Param('', type=['null', 'string']),
                'cnv_centric_release_id': Param('', type=['null', 'string']),
                'color': Param('', type=['null', 'string']),
            },
            default_args={
                'on_failure_callback': Slack.notify_task_failure,
                'trigger_rule': TriggerRule.NONE_FAILED,
            },
    ) as dag:
        def resolve_release_id(index: str) -> str:
            """
            Resolves the release ID for a given index.

            This function generates a Jinja template expression that retrieves the release ID for a specific parameter
            (e.g., `params.<index>_release_id`). If this specific release ID was not passed to the DAG, it defaults to
            the general `params.release_id`.
            """
            index_release_id = f'params.{index}_release_id'
            return f'{{% if {index_release_id} and {index_release_id}|length %}}' \
                   f'{{{{ {index_release_id} }}}}' \
                   '{% else %}{{ params.release_id or "" }}{% endif %}'


        params_validate = validate_color(color())
        env_color = params_validate.__str__()
        underscore_color = ('_' + env_color) if env_color else ''
        dash_color = ('-' + env_color) if env_color else ''

        get_release_ids_group = get_release_ids(
            release_id=release_id(),
            color=underscore_color,
            gene_centric_release_id=resolve_release_id('gene_centric'),
            gene_suggestions_release_id=resolve_release_id('gene_suggestions'),
            variant_centric_release_id=resolve_release_id('variant_centric'),
            variant_suggestions_release_id=resolve_release_id('variant_suggestions'),
            coverage_by_gene_centric_release_id=resolve_release_id('coverage_by_gene_centric'),
            cnv_centric_release_id=resolve_release_id('cnv_centric'),
            increment_release_id=False  # Get current release ID
        )

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
                        {{ "add": {{ "index": "clin_{env}{underscore_color}_gene_centric_{gene_centric_release_id}", "alias": "clin_{env}_gene_centric" }} }},
                        {{ "add": {{ "index": "clin_{env}{underscore_color}_gene_suggestions_{gene_suggestions_release_id}", "alias": "clin_{env}_gene_suggestions" }} }},
                        {{ "add": {{ "index": "clin_{env}{underscore_color}_variant_centric_{variant_centric_release_id}", "alias": "clin_{env}_variant_centric" }} }},
                        {{ "add": {{ "index": "clin_{env}{underscore_color}_variant_suggestions_{variant_suggestions_release_id}", "alias": "clin_{env}_variant_suggestions" }} }},
                        {{ "add": {{ "index": "clin_{env}{underscore_color}_coverage_by_gene_centric_{coverage_by_gene_centric_release_id}", "alias": "clin_{env}_coverage_by_gene_centric" }} }}
                    ]
                }}
                '''.format(
                    env=env,
                    dash_color=dash_color,
                    underscore_color=underscore_color,
                    # Release IDs returned by get_release_ids
                    gene_centric_release_id=release_id('gene_centric'),
                    gene_suggestions_release_id=release_id('gene_suggestions'),
                    variant_centric_release_id=release_id('variant_centric'),
                    variant_suggestions_release_id=release_id('variant_suggestions'),
                    coverage_by_gene_centric_release_id=release_id('coverage_by_gene_centric'),
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
                    {{ "add": {{ "index": "clin_{env}{underscore_color}_cnv_centric_{cnv_centric_release_id}", "alias": "clin_{env}_cnv_centric" }} }}
                ]
            }}
            '''.format(
                env=env,
                cnv_centric_release_id=release_id('cnv_centric'),  # Release ID returned by get_release_ids
                dash_color=dash_color,
                underscore_color=underscore_color,
            ),
        ],
        skip_fail_env=[Env.QA, Env.STAGING, Env.PROD],
    )

    arranger_remove_project_task = arranger.remove_project()
    arranger_restart_task = arranger.restart(on_success_callback=Slack.notify_dag_completion)

    (params_validate >> get_release_ids_group >> es_indices_swap >> es_cnv_centric_index_swap >>
     arranger_remove_project_task >> arranger_restart_task)
