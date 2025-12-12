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
from scripts import etl_import_igv_scripts
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
        max_active_tasks=4, # allowed several files to be downloaded simultaneously
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
    
    tabix_dir = "/tmp/tabix"
    bgzip_path = "/tmp/tabix/bin/bgzip"
    tabix_path = "/tmp/tabix/bin/tabix"

    '''
    Set of generic tasks repeatedly used to download + sanitize + save into S3
    '''

    @task.bash(task_id='download', cwd=work_dir())
    def download(url: str) -> str:
        return f"curl -f -O {url}"
    
    @task.bash(task_id='sanitize', cwd=work_dir())
    def sanitize(file_name: str, unzip: bool = True) -> str:
        # the files, even being .gz need to be bzipped explicitly for tabix
        unzip = f"gzip -d {file_name}" if (unzip) else "echo \"unzip=False\"" # note: that command delete the original file, perfeclty fine
        bzip = f"{bgzip_path} -f {file_name.removesuffix('.gz')}"
        tabix = f"{tabix_path} -p gff {file_name}"
        return unzip + " && " + bzip + " && " + tabix

    @task(task_id='save')
    def save(file_name: str, work_dir: str):
        s3.load_file(f"{work_dir}/{file_name}", f"igv/{file_name}", s3_public_bucket, replace=True)
        s3.load_file(f"{work_dir}/{file_name}.tbi", f"igv/{file_name}.tbi", s3_public_bucket, replace=True)

    '''
    Prepare the DAG with temporary directories + download bzip and tabix tools
    '''

    @task_group(group_id='prepare')
    def prepare_group():

        @task.bash(task_id='workdirs')
        def workdirs(reset: str, work_dir: str) -> str:
            cmd = f"rm -rf {tabix_dir} && " if reset == "yes" else ""
            return cmd + f"rm -rf {work_dir} && mkdir -p {work_dir} && mkdir -p {tabix_dir}"
        
        @task(task_id='tabix')
        def tabix():
            if not os.path.exists(tabix_path):

                downloaded_tabix_file = "tabix-0.2.6-ha92aebf_0.tar.bz2"
                downloaded_tabix_path = f"{tabix_dir}/{downloaded_tabix_file}"

                http_get_file(url=f"https://anaconda.org/bioconda/tabix/0.2.6/download/linux-64/{downloaded_tabix_file}", path=downloaded_tabix_path)

                logger.info(f"Downloaded tabix archive: {os.path.getsize(downloaded_tabix_path)} bytes")

                with tarfile.open(downloaded_tabix_path, 'r') as tar:
                    tar.extractall(path=tabix_dir)
                
                logger.info(f"Extracted archive content:")
                for subdir, _, files in os.walk(tabix_dir):
                    for file in files:
                       logger.info(f"{os.path.join(subdir, file)}")

            else:
                 logger.info(f"Tabix tools already installed")
        
        workdirs(reset=reset(), work_dir=work_dir()) >> tabix()

    '''
    Bellow are one group for each IGV file to download
    '''

    @task_group(group_id='dgv_gold_standard')
    def dgv_gold_standard_group():

        file = "DGV_GS_hg38.cleaned.sorted.gff3.gz"
        
        @task.bash(task_id='sanitize', cwd=work_dir())
        def sanitize() -> str:
            escape = "cat DGV.GS.hg38.gff3 | python3 -c 'import sys, re; [sys.stdout.write(re.sub(r\"%(?!([0-9a-fA-F]{2}))\", \"%25\", line)) for line in sys.stdin]' > DGV.GS.hg38.cleaned.gff3"
            sort = "sort -k1,1 -k4,4n DGV.GS.hg38.cleaned.gff3 > DGV_GS_hg38.cleaned.sorted.gff3"
            bzip = f"{bgzip_path} -f DGV_GS_hg38.cleaned.sorted.gff3"
            tabix = f"{tabix_path} -p gff {file}"
            return escape + " && " + sort + " && " + bzip + " && " + tabix

        download(url="https://dgv.tcag.ca/dgv/docs/DGV.GS.hg38.gff3") >> sanitize() >> save(file_name=file, work_dir=work_dir())

    @task_group(group_id='clinvar_nstd102_group')
    def clinvar_nstd102_group():

        file = "nstd102.GRCh38.variant_call.vcf.gz"

        download(url=f"https://ftp.ncbi.nlm.nih.gov/pub/dbVar/data/Homo_sapiens/by_study/vcf/{file}") >> sanitize(file_name=file) >> save(file_name=file, work_dir=work_dir())

    @task_group(group_id='clinvar_snv')
    def clinvar_snv_group():

        file = "clinvar.vcf.gz"
        
        download(url=f"https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/{file}") >> sanitize(file_name=file) >> save(file_name=file, work_dir=work_dir())

    @task_group(group_id='gnomad_4_1_structural_variants')
    def gnomad_4_1_structural_variants():

        file = "gnomad.v4.1.sv.sites.vcf.gz"
        
        download(url=f"https://storage.googleapis.com/gcp-public-data--gnomad/release/4.1/genome_sv/{file}") >> sanitize(file_name=file) >> save(file_name=file, work_dir=work_dir())

    @task_group(group_id='gnomad_4_1_cnv')
    def gnomad_4_1_cnv():

        file = "gnomad.v4.1.cnv.all.vcf.gz"
        
        download(url=f"https://storage.googleapis.com/gcp-public-data--gnomad/release/4.1/exome_cnv/{file}") >> sanitize(file_name=file) >> save(file_name=file, work_dir=work_dir())

    @task_group(group_id='clingen_gene_dosage_sensitivity')
    def clingen_gene_dosage_sensitivity():

        file = "ClinGen_gene_curation_list_GRCh38.sorted.gff3.gz"

        @task.bash(task_id='download_and_convert', cwd=work_dir())
        def download_and_convert() -> str:
            return etl_import_igv_scripts.clingen_gene_dosage_sensitivity
        
        download_and_convert() >> sanitize(file_name=file, unzip=False) >> save(file_name=file, work_dir=work_dir())

    @task_group(group_id='clingen_region_dosage_sensitivity')
    def clingen_region_dosage_sensitivity():

        file = "ClinGen_region_curation_list_GRCh38.sorted.gff3.gz"

        @task.bash(task_id='download_and_convert', cwd=work_dir())
        def download_and_convert() -> str:
            return etl_import_igv_scripts.clingen_region_dosage_sensitivity
        
        download_and_convert() >> sanitize(file_name=file, unzip=False) >> save(file_name=file, work_dir=work_dir())

    @task.bash(task_id='cleanup')
    def cleanup(work_dir: str) -> str:
        # mostly for debuging purpose but could be useful for monitoring
        logger.info(f"Content of work directory before cleanup:")
        for subdir, _, files in os.walk(work_dir):
            for file in files:
                logger.info(f"{os.path.join(subdir, file)}")
        return f"rm -rf {work_dir}"

    end_task = EmptyOperator(
        task_id="end",
        on_success_callback=Slack.notify_dag_completion
    )

    start_task >> prepare_group() >> [dgv_gold_standard_group(), clinvar_nstd102_group(), clinvar_snv_group(), gnomad_4_1_structural_variants(), gnomad_4_1_cnv(), clingen_gene_dosage_sensitivity(), clingen_region_dosage_sensitivity()] >> cleanup(work_dir=work_dir()) >> end_task
