# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

Requires Python 3.12. Use [pyenv](https://github.com/pyenv/pyenv) to manage versions.

```zsh
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Commands

```bash
# Run all tests
pytest

# Run a single test file
pytest tests/lib/tasks/test_batch_type.py

# Run a single test
pytest tests/lib/tasks/test_batch_type.py::test_name

# Skip slow tests
pytest -m "not slow"

# Skip tests requiring VPN
pytest -m "not vpn"
```

### Local Airflow dev stack

```zsh
make setup          # Create .env from .env.sample
make start          # Start Airflow + MinIO via Docker Compose

# Test a single task manually
docker-compose exec airflow-scheduler airflow tasks test <dag> <task> 2022-01-01
```

Airflow UI: http://localhost:50080 (user: `airflow`, pass: `airflow`)

After starting, import `variables.json` via Admin > Variables in the UI. Key variables:
- `environment`: `qa`
- `kubernetes_namespace`: `cqgc-qa`
- `kubernetes_context_default` / `kubernetes_context_etl`: kubeconfig context names
- `s3_conn_id`: `minio`

## Architecture

This is an **Apache Airflow** project containing ETL pipeline DAGs for the CQGC clinical genomics platform (CHU Sainte-Justine).

### Directory structure

```
dags/                    # All Airflow DAGs and shared library code
  etl_*.py               # Top-level DAG definitions
  test_*.py              # Test/debug DAGs (shown when show_test_dags=yes)
  lib/
    config.py            # Central config: env vars, image tags, bucket names, env-specific settings
    config_operators.py  # Pre-built operator configs (kube_config_etl, nextflow_base_config)
    datasets.py          # Airflow Dataset definitions (enriched_clinical Delta Lake table)
    utils_etl.py         # Core utilities: skip helpers, ETL argument builders, analysis type enums
    operators/           # Custom Airflow operators (Spark, FHIR, K8s, Slack, etc.)
    groups/              # Reusable task groups composed into DAGs
      ingest/            # ingest_germline, ingest_somatic_tumor_only, ingest_somatic_tumor_normal
      normalize/         # normalize_germline, normalize_somatic_tumor_normal, normalize_somatic_tumor_only
      migrate/           # migration task groups
      franklin/          # Franklin genomics integration (create/update)
      phenovar/          # Phenovar integration
      index/             # Elasticsearch index management
    tasks/               # Reusable @task functions (batch_type, normalize, clinical, etc.)
    doc/                 # DAG documentation strings
  sensors/               # Airflow sensors (franklin, rolling)
tests/                   # pytest tests (mirrors dags/lib/ structure)
  conftest.py            # Shared fixtures: MinIO testcontainers, Airflow Variable mocks
```

### Environment system

`dags/lib/config.py` reads Airflow Variables at import time and sets env-specific constants (image tags, JAR versions, ES URLs, etc.) for four environments: `test`, `qa`, `staging`, `prod`. All operators and task groups depend on these module-level constants.

S3 bucket names follow the pattern `cqgc-{env}-app-{purpose}` (e.g. `cqgc-qa-app-datalake`).

### Analysis types

Three clinical analysis types drive conditional logic throughout:
- `GERMLINE` (bioinfo code: `GEBA`) — standard germline variant analysis
- `SOMATIC_TUMOR_ONLY` (bioinfo code: `TEBA`) — somatic tumor without matched normal
- `SOMATIC_TUMOR_NORMAL` (bioinfo code: `TNEBA`) — somatic tumor with matched normal

The `detect_batch_type` task (in `dags/lib/tasks/batch_type.py`) determines the analysis type at runtime by querying the `enriched_clinical` Delta Lake table or falling back to the S3 metadata file. This result is passed via XCom to downstream tasks.

### Skip pattern

Tasks are conditionally skipped using Jinja-templated strings. An empty string `""` means "don't skip"; any non-empty string (conventionally `"yes"`) means "skip". The `skip()` helper in `utils_etl.py` concatenates two such strings (OR logic). This allows skip conditions to be composed and passed through task group hierarchies.

### Operator hierarchy

```
KubernetesPodOperator (Airflow)
  └── BaseKubernetesOperator   # Adds Jinja-templatable image_pull_secrets_name field
  └── SparkOperator            # Runs Spark jobs on K8s ETL cluster; handles pod/driver cleanup
        └── SparkETLOperator   # Adds batch_id/analysis_ids handling and batch type validation
```

`BaseKubernetesOperator` pairs with two companion dataclasses in `dags/lib/operators/base_kubernetes.py`:
- `KubeConfig` — holds cluster connection settings (`in_cluster`, `cluster_context`, `namespace`, `image_pull_secrets_name`)
- `BaseConfig` — fluent builder for operators; provides `.append_args()`, `.prepend_args()`, `.with_image()`, `.build_operator()`, and `.partial()` to construct operators without repeating boilerplate

New operators should use this builder system. Legacy operators inherit directly from `SparkOperator`.

### Key DAGs

The pipeline follows a chain of DAG triggers:

**`etl_run_release_trigger`** (scheduled daily at 1am UTC) — polls S3 for pending sequencing IDs; skips if `etl_run_release` or `etl` is already running; triggers `etl_run_release`.

**`etl_run`** — saves sequencing run metadata to S3 (called by the prescription API, not part of the trigger chain).

**`etl_run_release`** — the top-level release pipeline: FHIR ingest → Nextflow germline → Franklin/Phenovar imports → triggers `etl`.

**`etl`** — main orchestration DAG (scheduled weekly on QA, manual elsewhere): triggers `etl_ingest` per batch/analysis group, then runs enrich → prepare index → QA → get release IDs → index → publish → rolling → notify.

**`etl_ingest`** — single-batch ingest DAG; accepts `batch_id` or `analysis_ids`, detects analysis type, dispatches to germline/somatic task groups.

Other notable DAGs:
- `etl_rolling` / `etl_rolling_auto` — rolling re-ingestion over all known batches
- `etl_reset_enrich_*` — reset/recompute enrichment tables
- `etl_import_*` — import DAGs for public reference databases; several run on a weekly Saturday schedule (ClinVar, dbSNP, OMIM, Orphanet, HPO, DDD, Mondo, RefSeq, human genes), others are manual-only (gnomAD, COSMIC, dbNSFP, SpliceAI, etc.)
- `etl_arranger` — Elasticsearch/Arranger index management
- `etl_qc` / `etl_qc_es` — quality control DAGs

### Testing

Tests run against a real MinIO instance launched via `testcontainers`. The top-level `conftest.py` mocks Airflow Variables (set to `environment=test`) and provides `clin_minio`/`franklin_s3` fixtures that start MinIO containers and configure Airflow S3 connections automatically.

The `test` environment (`env == 'test'`) is used in CI/unit tests — it sets placeholder image names and nulls out cluster credentials so K8s calls are never made.
