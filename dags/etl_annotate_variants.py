# Name ok?

#1) Produce required input files
#2) Call variant post processing pipeline (nextflow)
#3) Move the output files at the right location and, if desired, save nextflow run info
#4) If desired,  import the variants (discuss with team if it is appropriate here)

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from lib.config import k8s_namespace, K8sContext, nextflow_service_account
from lib.operators.nextflow import NextflowOperator

with DAG(
    dag_id="etl_annotate_variants",
    start_date=datetime(2022, 1, 1),  #TODO: idem as other tasks, is it ok?
    schedule=None #Using instead schedule_interval, which is marked as deprecated
    #TODO: add parameters (input file, output dir, params)
):

    #TODO: make sure the id and name are appropriate
    NextflowOperator(
        task_id='nextflow-annotate-variant',
        name='nextflow-annotate-variant',
        k8s_context = K8sContext.ETL,
        persistent_volume_claim_name = f"{k8s_namespace}-nextflow-pvc",
        service_account_name = nextflow_service_account,
        arguments = [  #Here we should not keep the test config and params.json file in the final version
            "nextflow",
            "-c",
            "/opt/nextflow/config/nextflow.config", #We should move this file in the launch directory, this way we don't need to pass it
            "run",
            "-r",
            "feat/add-test-dataset",
            "-params-file",
            "/mnt/workspace/params.json",
            "Ferlab-Ste-Justine/cqdg-denovo-nextflow",
        ],
        on_finish_action="delete_succeeded_pod" #Strangely, this does not work for now. Perhaps the parent airflow task is ignoring?
    )
    
    #NEXT POC steps
    # - add the dag parameters and testwith the right interface (will need to modify the nextflow code to do so , ok to use a branch)
    # - mimic the full persistent volume setup agreed with devops team (need to cd in the right directory in the pod)
    # - calling via tasks.annotate_variants and polish nextflow operator (remove anything hard coded)
    # - recuperate and archive logs / state files?
    # - Try to figure out if we could pass an extra -resume arguments when re-running the job with airflow
    # - Unit tests + documentation if needed
    