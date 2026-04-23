phenovar = '''
# ETL Import Phenovar

Submit germline analyses to the **Phenovar** API for phenotype-driven variant
prioritization, poll for completion, and create **FHIR DocumentReferences**
linking to the results.

## Summarized workflow

1. Collect **batch_ids** and/or **analysis_ids** from params, validate, optionally
   run the FHIR ingest to refresh `enriched_clinical`
2. Reject anything that is not **GERMLINE**
3. Optionally clear previous S3 state (if `reset=yes`)
4. Rollout the phenovar deployment → `reset_all` fires on pod boot, wiping Redis
   queue + MySQL patient rows + data PVC, then writes a fresh `phenovar:boot_id`
5. **create**: extract clinical facts, copy VCFs to the phenovar-import bucket,
   submit every analysis in one pass
6. **update**: sensor polls `/check-status` every 5 min for up to 8 h. When a
   task reports SUCCESS, the sensor harvests the result JSON inline from the
   same response and writes it to S3 before marking the analysis SUCCESS —
   there is no separate download step. After every analysis is resolved, the
   group cleans up S3 markers + VCFs
7. The Java `AddPhenovarDocuments` Spark job turns every result JSON into a
   FHIR DocumentReference + Task.output entry
8. Slack notification

The DAG is retry-safe: S3 status markers drive idempotency — rerunning with the
same inputs skips already-successful analyses and resumes the rest.

## Architecture

```
params → ingest_fhir → identifier_to_type → phenovar_validate →
  get_all_analysis_ids → reset_task (optional) → rollout_restart_phenovar →
  create (extract_clinical_data → copy_vcfs → submit_analyses) →
  update (api_sensor → clean_up_clin → clean_up_phenovar) →
  prepare_analysis_ids → add_phenovar_documents (Spark) → slack
```

## Task-level behavior

### reset_task

When `reset=yes`, deletes all S3 state under
`raw/landing/phenovar/analysis_id=<id>/` for the given analyses. Skipped
otherwise. Used when a previous run produced bad data or Phenovar was upgraded.

### rollout_restart_phenovar

`K8sDeploymentRestartOperator` patches the `phenovar` Deployment with an updated
`kubectl.kubernetes.io/restartedAt` annotation and waits (up to 5 min) for the
new ReplicaSet to be fully available. On pod boot, `PHENOVAR_RESET_ON_START=true`
triggers the container's `reset_all` routine:

- Redis `FLUSHDB` on both broker and result backend
- MySQL `phenovar3db` patient rows wiped (cascades through File/FileGenerator/Job
  relations; does NOT touch `sgmdatacenterdb` reference data or auth/tokens)
- Filesystem: `patient-repository/*`, `tmp/phenovar3/restapi.*`,
  `log/celery-task-logs/*` deleted
- Writes a fresh `phenovar:boot_id` UUID to Redis — the anchor for the
  orphan-detection mechanism used by the sensor

### create

#### extract_clinical_data

Reads `enriched_clinical` (Delta). Builds one dict entry per analysis with
proband/maternal/paternal rows. Filters out:

- analyses that already have `phenovar_result.json` on S3 (idempotency)
- families with siblings (not supported by Phenovar)
- analyses whose proband lacks HPO codes
- analyses with no VCF input

#### copy_vcfs

Mirrors the SNV + CNV VCFs from the download bucket
(`cqgc-{env}-app-download`) into the Phenovar import bucket
(`cqgc-{env}-app-phenovar-import/<analysis_id>/`).

Phenovar file types:

| filetype             | source                          |
|----------------------|---------------------------------|
| `patient_called_snv` | proband SNV                     |
| `patient_called_cnv` | proband CNV                     |
| `maternal_called_snv`| mother SNV (if present)         |
| `maternal_called_cnv`| mother CNV (if present)         |
| `paternal_called_snv`| father SNV (if present)         |
| `paternal_called_cnv`| father CNV (if present)         |

#### submit_analyses

Builds the Phenovar API payload (see example below) and POSTs to
`/phenovar3/rest_api/dxtablegenerator/generate/` for every analysis. Writes
each returned `task_id` to S3 as a marker (`_PHENOVAR_TASK_ID_.txt`) plus
status `PENDING` (`_PHENOVAR_STATUS_.txt`). `can_submit_analysis` short-circuits
anything already marked SUCCESS on S3.

Example payload:

```json
{
    "schema_version": "1.0.0",
    "patient_details": {
        "externalid": "1261482",
        "sex": "M",
        "maternal_affected": false,
        "paternal_affected": false,
        "label": "",
        "cohort": "1261485"
    },
    "phenotype_hpo_code_list": ["HP:0000151", "HP:0001263"],
    "phenotype_onset_hpo_code": "HP:0003577",
    "download_specifications": {
        "download_source": "s3_minio",
        "details": {
            "s3_bucket_name": "cqgc-qa-app-phenovar-import"
        }
    },
    "analysis_files": [
        {"filepath": "1261485/254bad63.vcf.gz", "filetype": "patient_called_snv"},
        {"filepath": "1261485/135a23ed.vcf.gz", "filetype": "patient_called_cnv"}
    ]
}
```

`sex` is the FHIR gender mapped to `M` / `F`. `download_source` is `s3_minio`
(must match Phenovar server config). Filepath is prefixed with the `analysis_id`
to match the directory structure `copy_vcfs` writes.

### update

#### api_sensor (PhenotypingAPISensor)

Polls `/phenovar3/rest_api/dxtablegenerator/check-status/?task_id=<id>` every
5 minutes for up to 8 hours. Updates each analysis's S3 marker as the task
progresses through `PENDING` → `STARTED` → `SUCCESS`/`FAILURE`.

**Inline result harvest**: the `/check-status` SUCCESS response already contains
the full result JSON (`result` field). The sensor writes it to
`raw/landing/phenovar/analysis_id=<id>/phenovar_result.json` BEFORE flipping
the marker to SUCCESS, closing the window where a pod reboot between "saw
SUCCESS" and "downloaded result" would lose the result (redis wipe at pod
boot takes the celery task meta with it, and re-polling the task_id would
return a ghost PENDING).

**Crash-recovery shortcut**: on entry, the sensor checks whether any PENDING
/ STARTED analysis already has a result file on S3. If so, it just flips the
marker to SUCCESS — no /check-status call needed. This covers the rare case
where a prior poke wrote the result but crashed before updating the marker.

**Boot-ID orphan detection**: every `/check-status` response also carries
`boot_id`. The sensor compares against the boot_id captured at submit-time
(pushed to XCom by `submit_analyses`). A mismatch means the phenovar pod was
rolled out mid-analysis — Redis lost the task, the returned state is a ghost
`PENDING` that would never resolve — so the sensor marks that analysis FAILURE
and raises `AirflowFailException` immediately rather than waiting out the 8h
timeout.

#### clean_up_clin / clean_up_phenovar

After results are saved, deletes the S3 markers
(`_PHENOVAR_STATUS_.txt`, `_PHENOVAR_TASK_ID_.txt`) and the copied VCFs from
the Phenovar import bucket. `phenovar_result.json` is kept — it's the durable
artifact consumed by the downstream Spark job.

## S3 state layout

Per-analysis state lives under `s3://cqgc-{env}-app-datalake/raw/landing/phenovar/`:

```
analysis_id=1261485/
  _PHENOVAR_STATUS_.txt    ← "PENDING" | "STARTED" | "SUCCESS" | "FAILURE"
  _PHENOVAR_TASK_ID_.txt   ← phenovar-assigned celery task UUID
  phenovar_result.json     ← final result JSON (kept after markers are deleted)
```

## Earliest-onset selection

`phenotype_onset_hpo_code` is the earliest age-at-onset HPO code from the
proband's clinical signs, picked via this hierarchy:

1. HP:0030674 — Antenatal onset
2. HP:0003577 — Congenital onset
3. HP:0003623 — Neonatal onset
4. HP:0003593 — Infantile onset
5. HP:0011463 — Childhood onset
6. HP:0003621 — Juvenile onset
7. HP:0011462 — Young adult onset
8. HP:0003596 — Middle age onset
9. HP:0003584 — Late onset (Senior)

Falls back to `""` when no onset code is found (the field is optional per the
Phenovar API spec).

## FHIR integration (add_phenovar_documents)

Spark `PipelineOperator` running `bio.ferlab.clin.etl.AddPhenovarDocuments`
reads every `phenovar_result.json`, creates a `DocumentReference` of type
`PHENOVAR`, adds an entry to `Task.output`, and moves the JSON into the
downloadable files bucket.

## Parameters

- **batch_ids** — list of batch IDs (triggers FHIR import)
- **analysis_ids** — list of analysis IDs (skips FHIR import)
- **color** — FHIR environment color (`blue` / `green`)
- **import** — whether to run the import (`yes` / `no`)
- **reset** — delete existing Phenovar S3 data before running (`yes` / `no`)
- **spark_jar** — optional custom Spark JAR version

## Mandatory constraints

- **`max_active_tasks=1`** and **`max_active_runs=1`** on the DAG: phenovar
  deployment is a single replica shared across the DAG. Parallel tasks would
  rollout-race each other.

## Business rules enforced

- **Germline only** — `phenovar_validate` rejects somatic batches early
- **SOLO / DUO / TRIO only** — sibling families are filtered out
- **Proband with HPO required** — Phenovar API rejects empty HPO lists
- **At least one VCF** — SNV or CNV

## Retries and failure handling

- **Per-analysis failures don't block others** — the sensor surfaces a FAILURE
  at the end of its poke cycle; completed analyses stay successful on S3
- **Rerun with `reset=no`** replays only PENDING/FAILURE analyses, skipping
  already-SUCCESS ones
- **Rerun with `reset=yes`** wipes all S3 state for the given analyses and
  starts fresh

## Disk hygiene on the phenovar pod

After each SNV pipeline step completes, the server's entrypoint deletes the
raw input VCFs for proband + maternal + paternal from the patient repository.
The pipeline has already consumed them; downstream (CNV, create_dxtable,
response serialization) only reads the processed outputs
(`patient_processed_snv`, `patient_processed_cnv`, `diagnosistable`). This
keeps the data PVC footprint per analysis down to tens of MB even for
real-scale trios, so a single DAG run can submit many analyses without
blowing the PVC budget.

## SpliceAI / gnomAD / dbSNP reference-file shadowing

The phenovar container entrypoint shadows three `cqgcgenome` reference files
to their already-cropped `cqgcexome` equivalents on the resources PVC. The two
target BED files are byte-identical (same 242,137 rows / 35.79 Mb coverage), so
the shadow is lossless. Real measured impact per analysis:

- AddSpliceAI: ~2h 28m → ~50m
- AddSNPID: ~8m → ~2m 30s
- AddGnomad: ~5× smaller reference read
'''
