from datetime import datetime

from airflow import DAG

from lib.tasks.public_data import (PublicSourceDag, get_schedule_by_env,
                                   init_public_databases_dag_id, sync_public_databases_file)


with DAG(
    dag_id=init_public_databases_dag_id,
    start_date=datetime(2026, 7, 31),
    # Daily, ahead of the scheduled public source imports (the earliest is ClinVar at 6am) so a
    # source added or renamed by a deployment is registered before its next run.
    schedule=get_schedule_by_env('0 5 * * *'),
    default_args=PublicSourceDag.default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    sync_public_databases_file()
