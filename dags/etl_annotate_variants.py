from datetime import datetime
from airflow import DAG
from airflow.models.param import Param

from lib import config
from lib.tasks.nextflow import annotate_variants


# TODO: validate urls
DEFAULT_SAMPLESHEET_URL = f"s3://{config.clin_import_bucket}/nextflow/tests/datasets/Post-processing-Pipeline/V3/data-test/samplesheet.csv"
DEFAULT_OUTPUT_DIR = f"s3://{config.clin_import_bucket}/nextflow/tests/output/"


# TODO: add slack notification
# TODO: support for extra args
with DAG(
    dag_id="etl_annotate_variants",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    description="Performs joint genotyping, tags low-quality variants, "
                "apply normalization, annotates and prioritizes variants "
                "using VEP and Exomiser.",
    params={
        "input": Param(
            DEFAULT_SAMPLESHEET_URL,
            type="string",
            description="The URL of the input samplesheet (csv)."
        ),
        "outdir": Param(
            DEFAULT_OUTPUT_DIR,
            type="string",
            description="Output directory URL for storing results."
        )
        # "extra_args": Param(
        #    [],
        #    type=["null", "array"],
        #    description="Extra nextflow arguments (optional)",
        # )
    },
    render_template_as_native_obj=True
) as dag:
    annotate_variants(
        task_id="etl_annotate_variants",
        input="{{ params.input }}",
        outdir="{{ params.outdir }}"
    )
