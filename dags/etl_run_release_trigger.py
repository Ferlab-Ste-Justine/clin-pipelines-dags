from datetime import datetime
import logging
from airflow.settings import Session
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.tasks.run_sequencings import get_pending_sequencing_ids, get_pending_batch_ids
from lib.slack import Slack

with DAG(
        dag_id='etl_run_release_trigger',
        start_date=datetime(2022, 1, 1),
        schedule='0 1 * * *',  # Run every day at 8pm EST (1am UTC)
        catchup=False,
        render_template_as_native_obj=True,
        default_args={
            'trigger_rule': TriggerRule.NONE_FAILED,
            'on_failure_callback': Slack.notify_task_failure,
        },
) as dag:
    
    @task(task_id='check_should_skip_run')
    def check_should_skip_run(sequencing_ids: list, batch_ids: list) -> str:
        
        # Only trigger if we have sequencing_ids, sequencing are more prioritary than somatic normal
        # if (not sequencing_ids or len(sequencing_ids) == 0) and (not batch_ids or len(batch_ids) == 0):
        if not sequencing_ids or len(sequencing_ids) == 0:
            logging.info("Skipping trigger: No pending sequencing_ids found")
            return 'yes'
        
        session = Session()
        try:
            # Check for running instances of etl_run_release or etl
            running_dags = session.query(DagRun).filter(
                DagRun.dag_id.in_(['etl_run_release', 'etl', 'etl_ingest', 'etl_import_franklin', 'etl_import_phenovar', 'etl_cnv_frequencies']),
                DagRun.state == DagRunState.RUNNING
            ).all()

            # If the most recent etl DagRun has any failed/upstream_failed task, the
            # previous release didn't actually complete — even if its DagRun state
            # eventually resolved. Refuse to schedule a new one until the operator
            # investigates. Raising (not returning 'yes') surfaces this loudly via
            # the on_failure_callback Slack alert instead of quietly skipping.
            last_etl_run = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == 'etl',
                    DagRun.state.in_([DagRunState.SUCCESS, DagRunState.FAILED]),
                )
                .order_by(DagRun.execution_date.desc())
                .first()
            )
            if last_etl_run is not None:
                failed_tis = (
                    session.query(TaskInstance)
                    .filter(
                        TaskInstance.dag_id == 'etl',
                        TaskInstance.run_id == last_etl_run.run_id,
                        TaskInstance.state.in_([
                            TaskInstanceState.FAILED,
                            TaskInstanceState.UPSTREAM_FAILED,
                        ]),
                    )
                    .all()
                )
                if failed_tis:
                    failed_names = sorted({ti.task_id for ti in failed_tis})
                    raise AirflowFailException(
                        f"last release didnt finish in success: "
                        f"{len(failed_names)} task(s) failed in etl run "
                        f"{last_etl_run.run_id}: {failed_names}"
                    )


            if running_dags:
                dag_names = ', '.join([dr.dag_id for dr in running_dags])
                logging.info(f"Skipping trigger: {dag_names} is/are currently running")
                return 'yes'
            else:
                logging.info("No conflicting DAGs running and pending IDs found, proceeding with trigger")
                return ''
        finally:
            session.close()
    
    get_pending_sequencing_ids_task = get_pending_sequencing_ids()
    get_pending_batch_ids_task = get_pending_batch_ids()
    check_should_skip = check_should_skip_run(get_pending_sequencing_ids_task, get_pending_batch_ids_task)

    trigger_etl_run_release = TriggerDagRunOperator(
        task_id='trigger_etl_run_release',
        trigger_dag_id='etl_run_release',
        wait_for_completion=True, # One release at a time
        skip=check_should_skip,
        conf={
            'sequencing_ids': get_pending_sequencing_ids_task,
            'batch_ids': get_pending_batch_ids_task
            }
    )

    # no Slack notifications here, just quietly trigger the etl_run_release dag if there are pendings
    [get_pending_sequencing_ids_task, get_pending_batch_ids_task] >> check_should_skip >> trigger_etl_run_release
