from datetime import datetime
import logging
import os
from airflow.models.param import Param
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from lib.operators.curl import CurlOperator
from lib.slack import Slack
from lib.utils_s3 import http_get_file
from lib.tasks.public_data import s3, s3_public_bucket
from lib.utils import http_get_file
import tarfile

logger = logging.getLogger(__name__)

with DAG(
        dag_id='etl_import_igv',
        start_date=datetime(2022, 1, 1),
        schedule=None,
        catchup=False,
        params={
            'reset': Param('no', enum=['yes', 'no']),
        },
        default_args={
            'on_failure_callback': Slack.notify_task_failure,
            'trigger_rule': TriggerRule.NONE_FAILED,
        },
        max_active_tasks=1,
        max_active_runs=1,
) as dag:

    start_task = EmptyOperator(
        task_id="start",
        on_success_callback=Slack.notify_dag_start
    )

    def reset() -> str:
        return '{{ params.reset or "" }}'
    
    def work_dir() -> str:
        return '/tmp/airflow_dgv_{{ run_id }}'

    @task_group(group_id='dgv_gold_standard')
    def dgv_gold_standard_group():

        tabix_dir = "/tmp/tabix"
        bgzip_path = "/tmp/tabix/bin/bgzip"
        tabix_path = "/tmp/tabix/bin/tabix"
        
        @task.bash(task_id='prepare')
        def prepare(reset: str, work_dir: str) -> str:
            cmd = f"rm -rf {tabix_dir} && " if reset == "yes" else ""
            return cmd + f"rm -rf {work_dir} && mkdir -p {work_dir} && mkdir -p {tabix_dir}"
    
        preapre_task = prepare(reset=reset(), work_dir=work_dir())

        @task(task_id='prepare_tabix')
        def prepare_tabix():
            if not os.path.exists(tabix_path):

                downloaded_tabix_file = "tabix-0.2.6-ha92aebf_0.tar.bz2"
                downloaded_tabix_path = f"{tabix_dir}/{downloaded_tabix_file}"

                http_get_file(f"https://anaconda.org/bioconda/tabix/0.2.6/download/linux-64/{downloaded_tabix_file}", downloaded_tabix_path)

                logger.info(f"Downloaded tabix archive: {os.path.getsize(downloaded_tabix_path)} bytes")

                with tarfile.open(downloaded_tabix_path, 'r') as tar:
                    tar.extractall(path=tabix_dir)
                
                logger.info(f"Extracted archive content:")
                for subdir, _, files in os.walk(tabix_dir):
                    for file in files:
                       logger.info(f"{os.path.join(subdir, file)}")

            else:
                 logger.info(f"Tabix tools already installed")
        
        @task.bash(task_id='download', cwd=work_dir())
        def download() -> str:
            return "curl -f -O https://dgv.tcag.ca/dgv/docs/DGV.GS.hg38.gff3"
        
        @task.bash(task_id='sanitize', cwd=work_dir())
        def sanitize() -> str:
            escape = "cat DGV.GS.hg38.gff3 | python3 -c 'import sys, re; [sys.stdout.write(re.sub(r\"%(?!([0-9a-fA-F]{2}))\", \"%25\", line)) for line in sys.stdin]' > DGV.GS.hg38.cleaned.gff3"
            sort = "sort -k1,1 -k4,4n DGV.GS.hg38.cleaned.gff3 > DGV_GS_hg38.cleaned.sorted.gff3"
            bzip = f"{bgzip_path} -f DGV_GS_hg38.cleaned.sorted.gff3"
            tabix = f"{tabix_path} -p gff DGV_GS_hg38.cleaned.sorted.gff3.gz"
            return escape + " && " + sort + " && " + bzip + " && " + tabix

        @task(task_id='save')
        def save(work_dir: str):
            # moslty for debuging purpose but could be useful for monitoring
            logger.info(f"Content available in current work directory:")
            for subdir, _, files in os.walk(work_dir):
                for file in files:
                    logger.info(f"{os.path.join(subdir, file)}")

            file = "DGV_GS_hg38.cleaned.sorted.gff3.gz"
            s3.load_file(f"{work_dir}/{file}", f"igv/{file}", s3_public_bucket, replace=True)
            s3.load_file(f"{work_dir}/{file}.tbi", f"igv/{file}.tbi", s3_public_bucket, replace=True)

        save_task = save(work_dir=work_dir())

        @task.bash(task_id='cleanup')
        def cleanup(work_dir: str) -> str:
            return f"rm -rf {work_dir}"
        
        cleanup_task = cleanup(work_dir=work_dir())

        preapre_task >> prepare_tabix() >> download() >> sanitize() >> save_task >> cleanup_task


    end_task = EmptyOperator(
        task_id="end",
        on_success_callback=Slack.notify_dag_completion
    )

    start_task >> dgv_gold_standard_group() >> end_task
