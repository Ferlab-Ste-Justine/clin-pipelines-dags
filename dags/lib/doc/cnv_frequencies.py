cnv_frequencies = """
### ETL CNV Frequencies DAG

The **etl_cnv_frequencies** DAG enriches CNVs with frequencies computed using Nextflow SVClustering.

#### DAG Workflow

1. **params_validate**: Verifies that `color` parameter was passed to the DAG in QA environment.
1. **ingest_fhir** group: Exports, normalizes, and enriches FHIR data.
1. **prepare_svclustering**: Prepares the CSV input files for Nextflow: one for germline and one for somatic CNVs.
1. **run_svclustering**: Executes the SVClustering Nextflow pipeline: once for germline and once for somatic CNVs.
1. **normalize_svclustering**: Normalizes the clustering outputs into Delta tables and computes frequencies for each cluster.
1. **enrich_cnv**: Enriches CNVs with frequencies and parental origin.
1. **prepare_cnv_centric**: Prepares CNV for indexation into a `cnv_centric` Parquet table. 
1. **qa** group: performs several quality checks:
    - **no_dup_nextflow_svclustering_germline_task**: Ensures no duplicates in the Nextflow SVClustering Germline normalized Delta table.
    - **no_dup_nextflow_svclustering_somatic_task**: Ensures no duplicates in the Nextflow SVClustering Somatic normalized Delta table.
    - **no_dup_nextflow_svclustering_parental_origin**: Ensures no duplicates in Nextflow SVClustering parental origin normalized Delta table.
    - **no_dup_cnv_centric**: Confirms the CNV centric data is deduplicated before indexing.
1. **index_cnv_centric**: Indexes the `cnv_centric` table in Elasticsearch.
1. **publish_cnv_centric**: Publishes the `cnv_centric` index, making it available for production use. On successful completion, a Slack notification is sent to notify the team.
1. **delete_previous_release**: Deletes the previous release of the `cnv_centric` index to conserve storage.
1. **rolling**: Triggers the `etl_rolling` DAG to swap indices and restart Arranger.

#### Parameters

The DAG accepts the following parameters:
- `release_id`: Release ID for the `cnv_centric` index. If not provided, the DAG will retrieve the current `release_id` from Elasticsearch and increment it by 1.
- `color`: Only required in QA environment.
- `spark_jar`: Optional. Name of the Spark JAR file.

#### Scheduling

- The DAG runs daily in production environment to keep the CNV frequency indices up-to-date. In other environments, it does not run on a schedule and can be triggered manually.
"""
