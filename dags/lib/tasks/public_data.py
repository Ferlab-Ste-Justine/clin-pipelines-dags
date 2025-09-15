import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lib import config
from lib.config import env
from lib.tasks.should_continue import IS_NEW_VERSION_KEY
from multiprocessing import Lock

s3 = S3Hook(config.s3_conn_id)
s3_public_bucket = f'cqgc-{env}-app-public'
s3_public_data_file_key = 'public-databases.json'

lock = Lock()

@dataclass
class PublicSource:
    id: str
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
            id="1000_genomes",
            source="1000 Genomes Project",
            url="https://www.internationalgenome.org/home"
        ),
        PublicSource(
            id="topmed_bravo",
            source="BRAVO",
            url="https://legacy.bravo.sph.umich.edu/freeze8/hg38/about"
        ),
        PublicSource(
            id="cosmic",
            source="COSMIC",
            url="https://www.cosmickb.org/"
        ),
        PublicSource(
            id="dbnsfp",
            source="dbNSFP",
            url="https://www.dbnsfp.org/"
        ),
        PublicSource(
            id="ensembl",
            source="Ensembl",
            url="https://www.ensembl.org/info/data/index.html"
        ),
        PublicSource(
            id="VEP",
            source="Ensembl Variant Effect Predictor (Ensembl VEP)",
            url="https://useast.ensembl.org/info/docs/tools/vep/index.html"
        ),
        PublicSource(
            id="gene2phenotype-ddd",
            source="Gene2Phenotype",
            url="https://www.ebi.ac.uk/gene2phenotype/"
        ),
        PublicSource(
            id="gnomad_joint_v4",
            source="gnomAD Joint Frequency",
            url="https://gnomad.broadinstitute.org/"
        ),
        PublicSource(
            id="hpo",
            source="Human Phenotype Ontology (HPO)",
            url="https://hpo.jax.org/"
        ),
        PublicSource(
            id="mondo",
            source="Mondo Disease Ontology",
            url="https://mondo.monarchinitiative.org/"
        ),
        PublicSource(
            id="clinvar",
            source="NCBI ClinVar",
            url="https://www.ncbi.nlm.nih.gov/clinvar/"
        ),
        PublicSource(
            id="dbsnp",
            source="NCBI dbSNP",
            url="https://www.ncbi.nlm.nih.gov/snp/"
        ),
        PublicSource(
            id="human_genes",
            source="NCBI Gene",
            url="https://www.ncbi.nlm.nih.gov/gene"
        ),
        PublicSource(
            id="refseq_annotation",
            source="NCBI RefSeq",
            url="https://www.ncbi.nlm.nih.gov/refseq/"
        ),
        PublicSource(
            id="omim",
            source="OMIM",
            url="https://www.omim.org/"
        ),
        PublicSource(
            id="orphanet",
            source="orphadata",
            url="https://www.orphadata.com/"
        ),
        PublicSource(
            id="spliceai",
            source="SpliceAI",
            url="https://github.com/Illumina/SpliceAI"
        )
    ]

    # Save to S3
    s3.load_string(_public_sources_to_json(public_sources), s3_public_data_file_key, s3_public_bucket, replace=True)


def _get_public_data_json() -> list[PublicSource]:
    # Check if the file exists
    if not s3.check_for_key(s3_public_data_file_key, s3_public_bucket):
        _create_public_data_file()

    # Read and return the file content as json
    return _json_to_public_sources(s3.read_key(s3_public_data_file_key, s3_public_bucket))


@task
def update_public_data_entry_task(id, version, allow_no_version = False, **context):
    # The lock is automatically acquired and released when the with block is exited
    with lock:
        if not id:
            raise Exception("an id is required")
        
        if not version:
            if allow_no_version:
                logging.info(f"no version found for '{id}'")
            if not context['ti'].xcom_pull(key=IS_NEW_VERSION_KEY):
                logging.info(f"this is not a new version, public-database file won't be updated")
                raise AirflowSkipException()
            else:
                raise Exception("version is missing")

        # Check if the entry already exists
        public_sources = _get_public_data_json()
        for entry in public_sources:
            if entry.id == id:
                entry.lastUpdate = datetime.now().isoformat()
                entry.version = version if version else ""
                entry.frequency = context['dag'].schedule_interval if context['dag'].schedule_interval else ""
                break
        else:
            raise Exception(f"PublicSource entry '{id}' not found")

        # Save to S3
        s3.load_string(_public_sources_to_json(public_sources), s3_public_data_file_key, s3_public_bucket, replace=True)
