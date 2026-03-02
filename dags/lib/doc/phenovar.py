phenovar = '''
# ETL Import Phenovar

Request **Phenovar** API to generate phenotype-driven variant prioritization results for germline analyses.

## Summarized workflow:

- Get **batch_ids** or **analysis_ids** from the **params**
- Validate that either **batch_ids** or **analysis_ids** are provided, not both
- If **batch_ids** are provided, import them to FHIR
- Export FHIR data and update the **enriched_clinical** table
- Validate that all analysis types are **GERMLINE**
- Extract the required information (patient details, HPO codes, VCF URLs) from **enriched_clinical** table
- Copy VCF files (SNV and CNV) from download bucket to Phenovar import bucket
- Submit analysis requests to **Phenovar** API *(if not already done in a previous execution)*
- Poke periodically **Phenovar** to check **task statuses** and update completed analyses
- When all analyses are complete, download the result **JSON** files and save them to **S3**
- Create **DocumentReference** FHIR resources linking to Phenovar results
- All analyses are set to completed and we cleanup intermediate files

## Detailed workflow

The tasks have been developed to be robust to crashes and can be re-run **multiple times** without 
re-submitting analyses to **Phenovar**.

You can safely run this DAG with the same **batch_ids** or **analysis_ids** several times without 
spamming **Phenovar**.

### extract_clinical_data

Extract patient information, HPO codes, and VCF file URLs from **enriched_clinical** table:

```json
{
    "1261485": [
        {
            "analysis_id": "1261485",
            "sequencing_id": "1261482",
            "family_id": "LDM_FAM1",
            "aliquot_id": "16774",
            "is_proband": true,
            "father_aliquot_id": "16776",
            "mother_aliquot_id": "16775",
            "affected_status": true,
            "gender": "Male",
            "clinical_signs": ["HP:0000151", "HP:0001263"],
            "snv_vcf_germline_urls": "WrappedArray(s3a://cqgc-qa-app-download/blue/254bad63.vcf.gz)",
            "cnv_vcf_germline_urls": "WrappedArray(s3a://cqgc-qa-app-download/blue/135a23ed.vcf.gz)"
        },
        {
            "analysis_id": "1261485",
            "sequencing_id": "1261496",
            "family_id": "LDM_FAM1",
            "aliquot_id": "16775",
            "is_proband": false,
            "father_aliquot_id": null,
            "mother_aliquot_id": null,
            "affected_status": false,
            "gender": "Female",
            "clinical_signs": [],
            "snv_vcf_germline_urls": "WrappedArray(s3a://cqgc-qa-app-download/blue/00fba308.vcf.gz)",
            "cnv_vcf_germline_urls": "WrappedArray(s3a://cqgc-qa-app-download/blue/2fc95fde.vcf.gz)"
        }
    ]
}
```

Data is grouped by **analysis_id** to handle trios (multiple rows per analysis).

### copy_vcfs

Copy VCF files from the download bucket (`cqgc-{env}-app-download`) to the Phenovar import bucket 
(`cqgc-{env}-app-phenovar-import`).

For each analysis:
- **Proband**: SNV VCF + CNV VCF
- **Mother** (if present): SNV VCF + CNV VCF
- **Father** (if present): SNV VCF + CNV VCF

Files are mapped to Phenovar file types:
- `patient_called_snv` - Proband SNV
- `patient_called_cnv` - Proband CNV
- `maternal_called_snv` - Mother SNV
- `maternal_called_cnv` - Mother CNV
- `paternal_called_snv` - Father SNV
- `paternal_called_cnv` - Father CNV

### submit_analyses

Build Phenovar API payload and submit analysis requests.

Example payload:
```json
{
    "schema_version": "1.0.0",
    "patient_details": {
        "externalid": "1261482",
        "sex": "Male",
        "maternal_affected": false,
        "paternal_affected": false,
        "label": "",
        "cohort": "1261485"
    },
    "phenotype_hpo_code_list": ["HP:0000151", "HP:0001263"],
    "phenotype_onset_hpo_code": "",
    "download_specifications": {
        "download_source": "cqgc_s3",
        "details": {
            "s3_bucket_name": "cqgc-qa-app-phenovar-import",
            "s3_bucket_root_path": "1261485"
        }
    },
    "analysis_files": [
        {"filepath": "254bad63.vcf.gz", "filetype": "patient_called_snv"},
        {"filepath": "135a23ed.vcf.gz", "filetype": "patient_called_cnv"}
    ]
}
```

API response contains a `task_id` used for polling:
```json
{
    "message": "Processing of your request has been queued...",
    "task_id": "adb2c61c-a9d2-47c6-bae2-721602686079",
    "status_url": "/phenovar3/rest_api/dxtablegenerator/check-status/?task_id=..."
}
```

### Status tracking

Analysis state is tracked using S3 marker files in the datalake bucket:

```
raw/landing/phenovar/
  analysis_id=1261485/
    _PHENOVAR_STATUS_.txt     → "PENDING" | "STARTED" | "SUCCESS" | "FAILURE"
    _PHENOVAR_TASK_ID_.txt    → "adb2c61c-a9d2-47c6-bae2-721602686079"
    phenovar_result.json      → Downloaded result (when SUCCESS)
```

**Idempotency**: The DAG checks status before submitting. If an analysis already exists with 
SUCCESS status and has a result file, it will be skipped.

### api_sensor

Polls the Phenovar API every **5 minutes** (configurable) for up to **8 hours** (configurable).

Checks task status via:
```
GET /phenovar3/rest_api/dxtablegenerator/check-status/?task_id={task_id}
```

Updates S3 status markers as tasks progress from PENDING → STARTED → SUCCESS/FAILURE.

### download_results

When a task reaches SUCCESS status, downloads the JSON result from the Phenovar API response 
and saves it to S3:

```
raw/landing/phenovar/analysis_id=1261485/phenovar_result.json
```

### FHIR Integration

After download, a `PipelineOperator` task calls the Java class 
`bio.ferlab.clin.etl.AddPhenovarDocuments` to:

1. Create a **DocumentReference** FHIR resource with type `PHENOVAR`
2. Add an entry to **Task.output** referencing the DocumentReference
3. Move the result JSON to the downloadable files bucket

### Cleanup

After all analyses are complete:
1. **clean_up_clin**: Delete status marker files from datalake
2. **clean_up_phenovar**: Delete VCF files from Phenovar import bucket

## Parameters

- **batch_ids**: List of batch IDs to process (triggers FHIR import)
- **analysis_ids**: List of analysis IDs to process (skips FHIR import)
- **color**: FHIR environment color (e.g., "blue", "green")
- **import**: Whether to run the import (`yes` or `no`)
- **reset**: Whether to delete existing Phenovar data before running (`yes` or `no`)
- **spark_jar**: Optional custom Spark JAR version

## Constraints

- **Germline only**: Only GERMLINE analyses are allowed (validated early in the workflow)
- **Proband required**: Each analysis must have a proband with HPO codes
- **VCF files required**: SNV and CNV VCF files must exist for the proband
- **Timeout**: Default 8 hours for Phenovar processing (typical duration: up to 3 hours)

## Error Handling

- **Retry-safe**: Can be restarted without duplicating work
- **Partial failures**: Individual analysis failures don't block others
- **Status tracking**: Failed analyses marked with FAILURE status, can be reset and retried
- **Sensor timeout**: If Phenovar takes longer than timeout, sensor fails (can be restarted)

## Reset Functionality

Set **reset=yes** parameter to:
- Delete all Phenovar S3 data for the specified analysis IDs
- Allow resubmission of previously completed analyses
- Useful for reprocessing with updated data or after Phenovar improvements
'''
