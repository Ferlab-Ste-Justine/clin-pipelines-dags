# Variant Annotation Setup

This document provides instructions for setting up and running the `etl_annotate_variants` airflow dag with minikube.


## Procedure

### 1. Follow basic nextflow setup

Refer to the [Nextflow Setup](./nextflow_setup.md) for initial setup instructions.


### 2. Upload the variant annotation pieline test dataset to minio

Get a copy of the public dataset used to test this pipeline (`data-test.tar.gz`). 
You can request it in the slack channel #bioinfo. 

Extract the dataset within the current folder:
```
tar -xzf data-test.tar.gz
````

Within the `data-test` folder, create a file named `samplesheet.csv`
with the following content:
```
familyId,sample,sequencingType,gvcf,familyPheno
family1,NA07019,WGS,s3://cqgc-qa-app-files-import/nextflow/tests/datasets/Post-processing-Pipeline/V3/data-test/gvcf/NA07019.chr22_50000000-51000000.g.vcf.gz,s3://cqgc-qa-app-files-import/nextflow/tests/datasets/Post-processing-Pipeline/V3/data-test/pheno/family1.yml
family1,NA07022,WGS,s3://cqgc-qa-app-files-import/nextflow/tests/datasets/Post-processing-Pipeline/V3/data-test/gvcf/NA07022.chr22_50000000-51000000.g.vcf.gz,s3://cqgc-qa-app-files-import/nextflow/tests/datasets/Post-processing-Pipeline/V3/data-test/pheno/family1.yml
family1,NA07056,WGS,s3://cqgc-qa-app-files-import/nextflow/tests/datasets/Post-processing-Pipeline/V3/data-test/gvcf/NA07056.chr22_50000000-51000000.g.vcf.gz,s3://cqgc-qa-app-files-import/nextflow/tests/datasets/Post-processing-Pipeline/V3/data-test/pheno/family1.yml
family2,NA07019,WES,s3://cqgc-qa-app-files-import/nextflow/tests/datasets/Post-processing-Pipeline/V3/data-test/gvcf/NA07019.chr22_50000000-51000000.g.vcf.gz,s3://cqgc-qa-app-files-import/nextflow/tests/datasets/Post-processing-Pipeline/V3/data-test/pheno/family2.yml
```


In the minio UI, upload the `data-test` folder to the bucket `cqgc-qa-app-file-imports` under the path:
```
s3://cqgc-qa-app-file-import/nextflow/tests/datasets/Post-processing-Pipeline/V3/
```

Use the Upload folder button.

Ensure the path `s3://cqgc-qa-app-files-import/nextflow/tests/datasets/Post-processing-Pipeline/V3/data-test/samplesheet.csv``
exists in minio.

### 3. Create Config Map

Create the expected kubernetes ConfigMap for the variant annotation pipeline by running the following command:

```bash
kubectl apply -f doc/test/templates/nextflow/nextflow_config_variant_annotation.yaml -n cqgc-qa
```
