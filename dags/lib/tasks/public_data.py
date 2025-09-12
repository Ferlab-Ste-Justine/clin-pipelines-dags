from datetime import datetime
import logging
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
from lib import config
from lib.config import env
from multiprocessing import Lock

s3 = S3Hook(config.s3_conn_id)
s3_public_bucket = f'cqgc-{env}-app-public'
s3_public_data_file_key = 'public-databases.json'

lock = Lock()

def _create_public_data_file():
    # Create the default json
    json_content = [
        {
            "id": "1000_genomes",
            "source": "1000 Genomes Project",
            "url": "https://www.internationalgenome.org/home",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "topmed_bravo",
            "source": "BRAVO",
            "url": "https://legacy.bravo.sph.umich.edu/freeze8/hg38/about",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "cosmic",
            "source": "COSMIC",
            "url": "https://www.cosmickb.org/",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "dbnsfp",
            "source": "dbNSFP",
            "url": "https://www.dbnsfp.org/",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "ensembl",
            "source": "Ensembl",
            "url": "https://www.ensembl.org/info/data/index.html",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "VEP",
            "source": "Ensembl Variant Effect Predictor (Ensembl VEP)",
            "url": "https://useast.ensembl.org/info/docs/tools/vep/index.html",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "gene2phenotype-ddd",
            "source": "Gene2Phenotype",
            "url": "https://www.ebi.ac.uk/gene2phenotype/",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "gnomad_joint_v4",
            "source": "gnomAD Joint Frequency",
            "url": "https://gnomad.broadinstitute.org/",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "hpo",
            "source": "Human Phenotype Ontology (HPO)",
            "url": "https://hpo.jax.org/",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "mondo",
            "source": "Mondo Disease Ontology",
            "url": "https://mondo.monarchinitiative.org/",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "clinvar",
            "source": "NCBI ClinVar",
            "url": "https://www.ncbi.nlm.nih.gov/clinvar/",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "dbsnp",
            "source": "NCBI dbSNP",
            "url": "https://www.ncbi.nlm.nih.gov/snp/",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "human_genes",
            "source": "NCBI Gene",
            "url": "https://www.ncbi.nlm.nih.gov/gene",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "refseq_annotation",
            "source": "NCBI RefSeq",
            "url": "https://www.ncbi.nlm.nih.gov/refseq/",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "omim",
            "source": "OMIM",
            "url": "https://www.omim.org/",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "orphanet",
            "source": "orphadata",
            "url": "https://www.orphadata.com/",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        },
        {
            "id": "spliceai",
            "source": "SpliceAI",
            "url": "https://github.com/Illumina/SpliceAI",
            "version": "",
            "lastUpdate": "",
            "frequency": ""
        }
    ]

    # Save to S3
    s3.load_string(json.dumps(json_content), s3_public_data_file_key, s3_public_bucket, replace=True)


def _get_public_data_json():
    # Check if the file exists
    if not s3.check_for_key(s3_public_data_file_key, s3_public_bucket):
        _create_public_data_file()

    # Read and return the file content as json
    return json.loads(s3.read_key(s3_public_data_file_key, s3_public_bucket))

def push_version_to_xcom(version, context):
    context['ti'].xcom_push(key='public_data_file_version', value=version)

def update_public_data_entry(id, no_version = False, **context):
    # The lock is automatically acquired and released when the with block is exited
    with lock:
        if not id:
            raise Exception("an id is required")
        
        version = context['ti'].xcom_pull(key='public_data_file_version')
        if not version:
            if not no_version:
                raise Exception("version not found in XCom, make sure to call 'push_version_to_xcom' before this task")
            else:
                logging.warning(f"no version found for '{id}'")

        # Check if the entry already exists
        public_data = _get_public_data_json()
        for entry in public_data:
            if entry['id'] == id:
                entry['lastUpdate'] = datetime.now().isoformat()
                entry['version'] = version if version else ""
                entry['frequency'] = context['dag'].schedule_interval if context['dag'].schedule_interval else ""
                break
        else:
            raise Exception(f"Public data entry '{id}' not found")

        # Save to S3
        s3.load_string(json.dumps(public_data), s3_public_data_file_key, s3_public_bucket, replace=True)

def get_update_public_data_entry_task(id, no_version = False):
    return PythonOperator(
        task_id='update_public_data_entry',
        python_callable=update_public_data_entry,
        op_kwargs={'id': id, 'no_version': no_version}
    )
