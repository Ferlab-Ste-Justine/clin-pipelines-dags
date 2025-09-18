import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import DagRun
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.state import State
from lib import config
from lib.config import env
from lib.tasks.should_continue import IS_NEW_VERSION_KEY
from multiprocessing import Lock
from sqlalchemy import desc

s3 = S3Hook(config.s3_conn_id)
s3_public_bucket = f'cqgc-{env}-app-public'
s3_public_data_file_key = 'public-databases.json'

lock = Lock()

@dataclass
class PublicSource:
    dag_id: str
    source: int
    url: str
    version: str = ""
    lastUpdate: str = ""
    frequency: str = ""


def _public_sources_to_json(publicSources: list[PublicSource]) -> str:
    return json.dumps([asdict(item) for item in publicSources])


def _json_to_public_sources(json_string: str) -> list[PublicSource]:
    return [PublicSource(**item) for item in json.loads(json_string)]


def _create_public_data_file():
    # Create the default json
    public_sources: list[PublicSource] = [
        PublicSource(
            dag_id="etl_import_1000_genomes",
            source="1000 Genomes Project",
            url="https://www.internationalgenome.org/home"
        ),
        PublicSource(
            dag_id="etl_import_topmed_bravo",
            source="BRAVO",
            url="https://legacy.bravo.sph.umich.edu/freeze8/hg38/about"
        ),
        PublicSource(
            dag_id="etl_import_cosmic",
            source="COSMIC",
            url="https://www.cosmickb.org/"
        ),
        PublicSource(
            dag_id="etl_import_dbnsfp",
            source="dbNSFP",
            url="https://www.dbnsfp.org/"
        ),
        PublicSource(
            dag_id="etl_import_ensembl",
            source="Ensembl",
            url="https://www.ensembl.org/info/data/index.html"
        ),
        PublicSource(
            dag_id="etl_import_ddd",
            source="Gene2Phenotype",
            url="https://www.ebi.ac.uk/gene2phenotype/"
        ),
        PublicSource(
            dag_id="etl_import_gnomad_v4_joint",
            source="gnomAD Joint Frequency",
            url="https://gnomad.broadinstitute.org/"
        ),
        PublicSource(
            dag_id="etl_import_hpo_genes",
            source="Human Phenotype Ontology (HPO)",
            url="https://hpo.jax.org/"
        ),
        PublicSource(
            dag_id="etl_import_mondo",
            source="Mondo Disease Ontology",
            url="https://mondo.monarchinitiative.org/"
        ),
        PublicSource(
            dag_id="etl_import_clinvar",
            source="NCBI ClinVar",
            url="https://www.ncbi.nlm.nih.gov/clinvar/"
        ),
        PublicSource(
            dag_id="etl_import_dbsnp",
            source="NCBI dbSNP",
            url="https://www.ncbi.nlm.nih.gov/snp/"
        ),
        PublicSource(
            dag_id="etl_import_human_genes",
            source="NCBI Gene",
            url="https://www.ncbi.nlm.nih.gov/gene"
        ),
        PublicSource(
            dag_id="etl_import_refseq_annotation",
            source="NCBI RefSeq",
            url="https://www.ncbi.nlm.nih.gov/refseq/"
        ),
        PublicSource(
            dag_id="etl_import_omim",
            source="OMIM",
            url="https://www.omim.org/"
        ),
        PublicSource(
            dag_id="etl_import_orphanet",
            source="orphadata",
            url="https://www.orphadata.com/"
        ),
        PublicSource(
            dag_id="etl_import_spliceai",
            source="SpliceAI",
            url="https://github.com/Illumina/SpliceAI"
        )
    ]

    public_sources = _init_last_update(public_sources)

    # Save to S3
    s3.load_string(_public_sources_to_json(public_sources), s3_public_data_file_key, s3_public_bucket, replace=True)


def _init_last_update(publicSources: list[PublicSource]):
    updatedPublicSources = []
    for entry in publicSources:
        dag_successful_runs = DagRun.find(dag_id=entry.dag_id, state=State.SUCCESS)
        runs_dates = list(map(lambda dr: dr.end_date.isoformat(), dag_successful_runs))
        if not runs_dates:
            logging.warning(f"no successful run found for PublicSource entry '{entry.dag_id}'")
        else:
            logging.info(f"last successful runs found'{runs_dates}'")
            runs_dates.reverse()
            entry.lastUpdate = runs_dates[0]

        updatedPublicSources.append(entry)

    return updatedPublicSources


def _get_public_data_json() -> list[PublicSource]:
    # Check if the file exists
    if not s3.check_for_key(s3_public_data_file_key, s3_public_bucket):
        _create_public_data_file()

    # Read and return the file content as json
    return _json_to_public_sources(s3.read_key(s3_public_data_file_key, s3_public_bucket))


@task
def update_public_data_entry_task(version, allow_no_version = False, **context):
    # The lock is automatically acquired and released when the with block is exited
    dag_id = context['dag'].dag_id
    with lock:
        if not version:
            if allow_no_version:
                logging.info("no version found")
            if not context['ti'].xcom_pull(key=IS_NEW_VERSION_KEY):
                logging.info("this is not a new version, only updating 'lastUpdate'")
            else:
                raise Exception("version is missing")

        # Check if the entry already exists
        public_sources = _get_public_data_json()
        for entry in public_sources:
            if entry.dag_id == dag_id:
                entry.lastUpdate = datetime.now().isoformat()
                entry.version = version if version else entry.version
                entry.frequency = context['dag'].schedule_interval if context['dag'].schedule_interval else ""
                break
        else:
            raise Exception(f"PublicSource entry '{dag_id}' not found")

        # Save to S3
        s3.load_string(_public_sources_to_json(public_sources), s3_public_data_file_key, s3_public_bucket, replace=True)
