from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.trigger_rule import TriggerRule
from lib.operators.trigger_dagrun import TriggerDagRunOperator
from lib.tasks.run_sequencings import get_pending_sequencing_ids
from lib.slack import Slack

with DAG(
        dag_id='etl_run_release_trigger',
        start_date=datetime(2022, 1, 1),
        schedule='0 1 * * *',  # Run every day at 8pm EST (1am UTC)
        catchup=False,
        render_template_as_native_obj=True,
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
        },
        max_active_runs=1  # Prevents multiple runs from stacking up
) as dag:
    
    @task(task_id='check_if_etl_running')
    def check_if_etl_running() -> str:
        """Check if etl_run_release or etl DAGs are currently running."""
        from airflow.settings import Session
        
        session = Session()
        try:
            # Check for running instances of etl_run_release or etl
            running_dags = session.query(DagRun).filter(
                DagRun.dag_id.in_(['etl_run_release', 'etl']),
                DagRun.state == DagRunState.RUNNING
            ).all()
            
            if running_dags:
                dag_names = ', '.join([dr.dag_id for dr in running_dags])
                print(f"Skipping trigger: {dag_names} is/are currently running")
                return 'yes'
            else:
                print("No conflicting DAGs running, proceeding with trigger")
                return ''
        finally:
            session.close()
    
    get_pendings = get_pending_sequencing_ids(skipIfEmpty=True)
    check_running = check_if_etl_running()

    trigger_etl_run_release = TriggerDagRunOperator(
        task_id='trigger_etl_run_release',
        trigger_dag_id='etl_run_release',
        wait_for_completion=True, # One release at a time
        skip=check_running,
        conf={
            'sequencing_ids': get_pendings
            }
    )

    # no Slack notifications here, just quietly trigger the etl_run_release dag if there are pendings
    get_pendings >> check_running >> trigger_etl_run_release
