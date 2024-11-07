rolling = """
### ETL Rolling DAG

The **etl_rolling** DAG swaps ElasticSearch indices and restarts Arranger.

#### Parameters

- `release_id`: A general release ID used when specific IDs are not provided.
- `gene_centric_release_id`, `variant_centric_release_id`, etc.: Specific release IDs for each index.
- `color`: Only required in QA environment.

#### Notes

- If no IDs are provided, the DAG fetches the current `release_id` for each index directly from ElasticSearch.
"""
