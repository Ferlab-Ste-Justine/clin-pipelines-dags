# Name ok?

#1) Produce required input files
#2) Call variant post processing pipeline (nextflow)
#3) Move the output files at the right location and, if desired, save nextflow run info
#4) If desired,  import the variants (discuss with team if it is appropriate here)


from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from lib.config import K8sContext
from lib.operators.nextflow import NextflowOperator

with DAG(
    dag_id="etl_annotate_variants",
    start_date=datetime(2022, 1, 1),  #TODO: idem as other tasks, is it ok?
    schedule=None #Using instead schedule_interval, which is marked as deprecated
    #TODO: add parameters (ex: input file, output dir, etc.)
):

    NextflowOperator(
        task_id='nextflow-hello',
        name='etl-nextflow-hello',
        k8s_context = K8sContext.ETL,
        service_account_name = "nextflow", 
        arguments = [
            "nextflow",
            "run",
            "nextflow-io/hello",
            "-c",
            "/opt/nextflow/config/nextflow.config"
        ]
    )

    #NEXT POC steps:
    # - run our variant post processing pipeline instead nextflow hello
    # - calling via tasks.annotate_variants
    # - recuperate and archive logs / state files
    # - Try to figure out if we could pass an extra -resume arguments when re-running the job with airflow
    # - Unit tests + documentation if needed
    