from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.tasks.run_sequencings import check_pending_sequencing_ids, get_pending_sequencing_ids
from lib.slack import Slack

with DAG(
        dag_id='etl_run_release_trigger',
        start_date=datetime(2022, 1, 1),
        schedule='0 20 * * *',  # Run every day at 8pm
        catchup=False,
        render_template_as_native_obj=True,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_runs=1  # Prevents multiple runs from stacking up
) as dag:
    
    check_pendings = check_pending_sequencing_ids()
    get_pendings = get_pending_sequencing_ids()

    trigger_etl_run_release = TriggerDagRunOperator(
        task_id='trigger_etl_run_release',
        trigger_dag_id='etl_run_release',
        wait_for_completion=True, # One release at a time
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Only run if check task succeeded (not skipped)
        conf={
            'sequencing_ids': get_pendings
            }
    )

    # no Slack notifications here, just quietly trigger the etl_run_release dag if there are pendings
    check_pendings >> get_pendings >> trigger_etl_run_release
