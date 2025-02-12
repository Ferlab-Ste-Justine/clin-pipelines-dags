cnv_frequencies = """
### ETL CNV Frequencies DAG

The **etl_cnv_frequencies** DAG enriches CNVs with frequencies computed using Nextflow svclustering job.

#### DAG Workflow

1. **params_validate**: Verifies that `color` parameter was passed to the DAG in QA environment.
1. **prepare_svclustering**: Prepares the CSV input file for the SVClustering process.
1. **run_svclustering**: Executes the SVClustering Nextflow pipeline.
1. **normalize_svclustering**: Normalizes the clustering output into a Delta table and computes frequencies for each cluster in the table.
1. **enrich_cnv**: Enriches CNVs with frequencies and parental origin.
1. **prepare_cnv_centric**: Prepares CNV for indexation into a `cnv_centric` Parquet table. 
1. **qa** group: performs several quality checks:
    - **no_dup_nextflow_svclustering**: Ensures no duplicates in the Nextflow SVClustering normalized Delta table.
    - **no_dup_nextflow_svclustering_parental_origin**: Ensures no duplicates in Nextflow SVClustering parental origin normalized Delta table.
    - **no_dup_cnv_centric**: Confirms the CNV centric data is deduplicated before indexing.
1. **index_cnv_centric**: Indexes the `cnv_centric` table in Elasticsearch.
1. **publish_cnv_centric**: Publishes the `cnv_centric` index, making it available for production use. On successful completion, a Slack notification is sent to notify the team.

#### Parameters

The DAG accepts the following parameters:
- `release_id`: Release ID for the `cnv_centric` index. If not provided, the DAG will retrieve the current `release_id` from Elasticsearch and increment it by 1.
- `color`: Only required in QA environment.
- `spark_jar`: Optional. Name of the Spark JAR file.

#### Scheduling

- The DAG runs daily in production environment to keep the CNV frequency indices up-to-date. In other environments, it does not run on a schedule and can be triggered manually.
"""
