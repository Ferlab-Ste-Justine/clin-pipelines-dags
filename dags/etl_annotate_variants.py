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

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

with DAG(
    dag_id="etl_annotate_variants",
    start_date=datetime(2022, 1, 1),  #TODO: idem as other tasks, is it ok?
    schedule=None #Using instead schedule_interval, which is marked as deprecated
    #TODO: add parameters (ex: input file, output dir, etc.)
):

    #Step 1: trying with directly instantiated KuberneteesPodOperator
     KubernetesPodOperator(namespace='cqgc-qa',
                          image="python:3.6",
                          cmds=["python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="passing-test",
                          task_id="passing-task",
                          get_logs=True,
                          in_cluster=False,
                          cluster_context="minikube")

    #Step2: trying with my nextflow operator
    #NextflowOperator(
    #    task_id='nextflow-version',
    #    name='etl-nextflow-version',
    #    k8s_context = K8sContext.ETL,  #DOUBLE CHECK THIS
    #    arguments = [
    #        "nextflow",
    #        "-version"
    #    ],
    #    get_logs=True  #Not sure this is necessary
         # We might need to set in_cluster?
    #)

    #Step3: run real pipeline and not just nextflow -version command
    #Step4: calling via tasks.annotate_variants
  
    #Step5: recuperate and archive logs / state files
    #Step6: Try to figure out if we could pass an extra -resume arguments when re-running the job with airflow ...
    #(assuming we have a persistent volume that we can attach)

    #Step7: Unit tests + documentation if needed
    