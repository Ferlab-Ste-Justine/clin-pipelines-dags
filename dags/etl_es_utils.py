from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.config import K8sContext, env, es_url
from lib.operators.curl import CurlOperator
from lib.slack import Slack
from lib.tasks import es
from lib.tasks.params_validate import validate_color
from lib.utils_es import format_es_url
from lib.utils_etl import color, release_id, skip_if_param_not

with DAG(
        dag_id='etl_es_utils',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        params={
            'delete_release': Param('no', enum=['yes', 'no']),
            'delete_variant_release': Param('no', enum=['yes', 'no']),
            'test_duplicated_variants': Param('no', enum=['yes', 'no']),
            'show_indexes': Param('no', enum=['yes', 'no']),
            'test_disk_usage': Param('no', enum=['yes', 'no']),
            'release_id': Param('', type=['null', 'string']),
            'color': Param('', type=['null', 'string']),
        },
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
            'trigger_rule': TriggerRule.ALL_DONE,   # as opposed to other dags, we run everything even if a previous task fails
        },
        max_active_tasks=1,
        max_active_runs=1,
) as dag:


    def show_indexes() -> str:
        return '{{ params.show_indexes or "" }}'

    def test_disk_usage() -> str:
        return '{{ params.test_disk_usage or "" }}'

    def delete_release() -> str:
        return '{{ params.delete_release or "" }}'
    
    def delete_variant_release() -> str:
        return '{{ params.delete_variant_release or "" }}'

    def test_duplicated_variants() -> str:
        return '{{ params.test_duplicated_variants or "" }}'

    params_validate = validate_color(color())


    @task(task_id='params_action_validate')
    def validate_action_params(_delete_release, _delete_variant_release, _test_duplicated_variants, _release_id):
        if (_delete_release == 'yes' or _delete_variant_release == 'yes' or _test_duplicated_variants == 'yes') and _release_id == '':
            raise AirflowFailException('release_id is required for delete_release')

    params_action_validate = validate_action_params(delete_release(), delete_variant_release(), test_duplicated_variants(), release_id())

    es_delete_release = CurlOperator(
        task_id='es_delete_release',
        name='es-delete-release',
        k8s_context=K8sContext.DEFAULT,
        skip=skip_if_param_not(delete_release(), "yes"),
        arguments=[
            '-k', '--location', '--request', 'DELETE', '{es_url}/clin_{env}{under_color}_gene_suggestions_{release_id},clin_{env}{under_color}_variant_suggestions_{release_id},clin_{env}{under_color}_gene_centric_{release_id},clin_{env}{under_color}_variant_centric_{release_id},clin_{env}{under_color}_cnv_centric_{release_id},clin_{env}{under_color}_coverage_by_gene_centric_{release_id}?ignore_unavailable=true'
            .format(
                es_url=es_url,
                env=env,
                release_id=release_id(),
                under_color=color('_'),
                ),
        ],
    )

    es_delete_variant_release = CurlOperator(
        task_id='es_delete_variant_release',
        name='es-delete-variant-release',
        k8s_context=K8sContext.DEFAULT,
        skip=skip_if_param_not(delete_variant_release(), "yes"),
        arguments=[
            '-k', '--location', '--request', 'DELETE', '{es_url}/clin_{env}{under_color}_variant_centric_{release_id}?ignore_unavailable=true'
            .format(
                es_url=es_url,
                env=env,
                release_id=release_id(),
                under_color=color('_'),
                ),
        ],
    )

    es_test_duplicated_release_variant = es.test_duplicated_by_url \
        .override(task_id='es_test_duplicated_release_variant')(
            url=format_es_url('variant_centric', _color=color("_"), release_id=release_id(), suffix="/_search"),
            skip=skip_if_param_not(test_duplicated_variants(), "yes")
        )

    es_test_duplicated_release_cnv = es.test_duplicated_by_url \
        .override(task_id='es_test_duplicated_release_cnv')(
            url=format_es_url('cnv_centric', _color=color("_"), release_id=release_id(), suffix="/_search"),
            skip=skip_if_param_not(test_duplicated_variants(), "yes")
        )

    es_list_indexes = CurlOperator(
        task_id='es_list_indexes',
        name='es-list-indexes',
        k8s_context=K8sContext.DEFAULT,
        skip=skip_if_param_not(show_indexes(), "yes"),
        arguments=[
            '-k', '--location', '--request', 'GET', f'{es_url}/_cat/indices?h=idx'
        ],
    )

    es_test_disk_usage = es.test_disk_usage(skip= skip_if_param_not(test_disk_usage(), "yes"))

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    params_validate >> params_action_validate >> es_delete_release >> es_delete_variant_release >> es_test_duplicated_release_variant >> es_test_duplicated_release_cnv >> es_list_indexes >> es_test_disk_usage >> slack
