franklin='''
# ETL Import Franklin

Request distant API **Franklin** to create analyses based on families.

## Summarized workflow:

- Get **batch_ids** or **sequencing_ids** from the **params**
- Validate that either **batch_ids** or **sequencing_ids** are provided, not both
- If **batch_ids** are provided, import them to Fhir
- Export Fhir data and update the **enriched_clinical** table
- Validate that all analysis types are **GERMLINE**
- Extract the required information from the **enriched_clinical** table
- Request **Franklin** to create the analyses *(if not already done in a previous execution)*
- Poke periodically **Franklin** to see the **statuses** and update the ready analyses
- When all analyses are ready with download and save the result **JSON** in **S3**
- All analyses are set to completed and we cleanup intermediate files.

## Detailed workflow

The tasks has been developed to be robust to crashes and be re-run **X times** without requesting **Franklin** to re-create analyses.

You can safely run that DAG with the same **batch_ids** or **sequencing_ids** several times without spamming **Franklin**.

### group_families

Extract families and solo from **enriched_clinical** table such as below:
```json
{
    "families": {
        "TRIO_FAM1": [
            {
                ...
                "aliquot_id": "Trio_Prob",
                "family_id": "TRIO_FAM1",
                "analysis_id": "SRA0001",
                "is_proband": true,
                "father_aliquot_id": "Trio_Fth",
                "mother_aliquot_id": "Trio_Mth",
                ...
            },
            {
                ...
                "aliquot_id": "Trio_Mth",
                "family_id": "TRIO_FAM1",
                "analysis_id": "SRA0001",
                "is_proband": false,
                "father_aliquot_id": null,
                "mother_aliquot_id": null,
                ...
            },{
                ...
                "aliquot_id": "Trio_Fth",
                "family_id": "TRIO_FAM1",
                "analysis_id": "SRA0001",
                "is_proband": false,
                "father_aliquot_id": null,
                "mother_aliquot_id": null,
                ...
            },
        ]
        "TRIO_FAM2": [...]
    }
    "no_family": [
        {
            ...
            "aliquot_id": "Solo_Prob",
            "family_id": null,
            "analysis_id": "SRA0002",
            "is_proband": true,
            ...
        },
        {...}
    ]
}
```

### vcf_to_analyses

We extract all the VCF prefixes from the Nextflow bucket and try to attach them to the analyses using the proband
**aliquot_id**. If no VCF matches for an analysis, we fallback to the legacy **batch_id** folder in the **import** bucket 
and try to match to the analysis using the **aliquot_id** or the **family_id** or to the only VCF in the batch.

### create_analyses

Copy every relevant **VCFs** from our **S3** to **S3 Franklin**.

Request **Franklin** to create every analyses based on **group_families**

Analyses already with a **_FRANKLIN_STATUS.txt** or with a valid **analysis.json** (meaning **COMPLETED** in the past) are ignored.

For every created analyses we write the current received **IDs** and **STATUS** on **S3** to remember:

Example for TRIO:

```
/raw/landing/franklin/analysis_id=SRA0001/_FRANKLIN_IDS.txt (1,2,3,4)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Prob/_FRANKLIN_STATUS.txt (CREATED)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Mth/_FRANKLIN_STATUS.txt (CREATED)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Fth/_FRANKLIN_STATUS.txt (CREATED)
```

Two important remarks:

- at that point we dont have the correlation between **IDs** <=> **aliquot_id** the API response only contains an array of identifiers
- the ID **4** is a special family analysis requested, it's not a typo

Example for SOLO:

```
/raw/landing/franklin/analysis_id=SRA0002/_FRANKLIN_IDS.txt (5)

/raw/landing/franklin/analysis_id=SRA0002/aliquot_id=Solo_Prob/_FRANKLIN_STATUS.txt (CREATED)
```

### api_sensor

We **poke** Franklin API periodically for every analysis with _FRANKLIN_STATUS.txt (CREATED)

That status payload contains a link with previous **ID** <=> *aliquot_id* so we can save that information in **_FRANKLIN_ID.txt**

When an analysis status is **READY** we S3 as such:

```
/raw/landing/franklin/analysis_id=SRA0001/_FRANKLIN_IDS.txt (1,2,3,4)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Prob/_FRANKLIN_STATUS.txt (CREATED)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Mth/_FRANKLIN_ID.txt (2)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Mth/_FRANKLIN_STATUS.txt (READY)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Fth/_FRANKLIN_STATUS.txt (CREATED)

/raw/landing/franklin/analysis_id=SRA0002/_FRANKLIN_IDS.txt (5)

/raw/landing/franklin/analysis_id=SRA0002/aliquot_id=Solo_Prob/_FRANKLIN_ID.txt (5)

/raw/landing/franklin/analysis_id=SRA0002/aliquot_id=Solo_Prob/_FRANKLIN_STATUS.txt (READY)
```

we could ignore **_FRANKLIN_ID.txt** for **SOLO** as it will always be the same as **_FRANKLIN_IDS.txt** but code work the same.

If analyses are still not **ALL READY** after a timeout **AirflowFailException** is raised

Special case for the family analysis **4** we save it with **aliquot_id=null** when READY:

```
/raw/landing/franklin/analysis_id=SRA0002/aliquot_id=null/_FRANKLIN_ID.txt (4)

/raw/landing/franklin/analysis_id=SRA0002/aliquot_id=null/_FRANKLIN_STATUS.txt (READY)
```

### download

We get the **JSON** from Franklin API for every analysis with _FRANKLIN_STATUS.txt (READY). Status and then set to **COMPLETED**

S3 will be updated like:

```
/raw/landing/franklin/analysis_id=SRA0001/_FRANKLIN_IDS.txt (1,2,3,4)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Prob/_FRANKLIN_ID.txt (1)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Prob/_FRANKLIN_STATUS.txt (COMPLETED)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Prob/franklin_analysis_id=1/analysis.json

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Mth/_FRANKLIN_ID.txt (2)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Mth/_FRANKLIN_STATUS.txt (COMPLETED)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Mth/franklin_analysis_id=2/analysis.json

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Fth/_FRANKLIN_ID.txt (3)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Fth/_FRANKLIN_STATUS.txt (COMPLETED)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Fth/franklin_analysis_id=3/analysis.json

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=null/_FRANKLIN_ID.txt (4)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=null/_FRANKLIN_STATUS.txt (COMPLETED)

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=null/franklin_analysis_id=4/analysis.json

/raw/landing/franklin/analysis_id=SRA0002/_FRANKLIN_IDS.txt (5)

/raw/landing/franklin/analysis_id=SRA0002/aliquot_id=Solo_Prob/_FRANKLIN_ID.txt (5)

/raw/landing/franklin/analysis_id=SRA0002/aliquot_id=Solo_Prob/_FRANKLIN_STATUS.txt (COMPLETED)

/raw/landing/franklin/analysis_id=SRA0002/aliquot_id=Solo_Prob/franklin_analysis_id=5/analysis.json
```

### clean_up

That task purpose is to remove every **_FRANKLIN_xxx.txt** file for every analysis with a **COMPLETED** status 
so the **ETLs** can have a clean folders structure.

```
/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Prob/franklin_analysis_id=1/analysis.json

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Mth/franklin_analysis_id=2/analysis.json

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=Trio_Fth/franklin_analysis_id=3/analysis.json

/raw/landing/franklin/analysis_id=SRA0001/aliquot_id=null/franklin_analysis_id=4/analysis.json

/raw/landing/franklin/analysis_id=SRA0002/aliquot_id=Solo_Prob/franklin_analysis_id=5/analysis.json
```

Note that **create_analyses** will not request new analyses if run again in such condition.

## Problematics

Some problematics appeared during dev:

**Parts of the DAG will be re-used in etl, etl_ingest and etl_migrate or others**

**Solution:** create two re-usable groups: create and update


**Mapping between VCFs <=> aliquot IDs isnt that easy ... **

**Solution:** we try to match the VCF name prefix with aliquot ID or family ID or if it's the only VCF in the batch


**We dont want to request Franklin to create already done analyses**

**Solution:** *create_analyses* requests only analyses with no past-execution


**DAG can crashes and re-run X-times**

**Solution:** *_FRANKLIN_xxx.txt* files are saved on S3 to store the state of past-executions status, ids ...

'''