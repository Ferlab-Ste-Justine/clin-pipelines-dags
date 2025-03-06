from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import task
from airflow.utils.trigger_rule import TriggerRule
from lib.config import K8sContext, env, es_url
from lib.groups.index.delete_previous_releases import delete_previous_releases
from lib.groups.index.get_release_ids import get_release_ids
from lib.operators.curl import CurlOperator
from lib.slack import Slack
from lib.tasks import es
from lib.tasks.params_validate import validate_color
from lib.utils_es import format_es_url
from lib.utils_etl import color, release_id, skip_if_param_not

with DAG(
        dag_id='etl_delete_previous_releases',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
        params={
            'release_id': Param('', type=['null', 'string']),
            'color': Param('', type=['null', 'string']),
        },
         default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_tasks=6,
        max_active_runs=1,
) as dag:

    params_validate = validate_color(color())

    get_release_ids_group = get_release_ids(
        release_id=release_id(),
        color=color('_'),
        increment_release_id=False,
    )

    delete_previous_releases = delete_previous_releases(
        gene_centric_release_id=release_id('gene_centric'),
        gene_suggestions_release_id=release_id('gene_suggestions'),
        variant_centric_release_id=release_id('variant_centric'),
        variant_suggestions_release_id=release_id('variant_suggestions'),
        coverage_by_gene_centric_release_id=release_id('coverage_by_gene_centric'),
        cnv_centric_release_id=release_id('cnv_centric'),
        color=color('_'),
        skip=''
    )

    slack = EmptyOperator(
        task_id="slack",
        on_success_callback=Slack.notify_dag_completion
    )

    params_validate >> get_release_ids_group >> delete_previous_releases >> slack
