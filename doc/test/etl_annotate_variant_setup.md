# Setup to test etl_annotate_variant dag

This guide describes the steps to set up and run the `etl_annotate_variant` DAG locally using Minikube.


## Prerequisites

Follow the procedure in [nextflow_setup.md](nextflow_setup.md)

## Step 1: Download and prepare a test dataset

A public dataset have been created to test the nextflow pipeline involved in the etl_annotate_variant dag.
We shall download it and tweak it slightly. This only needs to be done once. 

First follow the documentation here to download a copy:
https://github.com/Ferlab-Ste-Justine/cqdg-denovo-nextflow/tree/feat/add-test-dataset?tab=readme-ov-file#testing-locally


Then do the following to modify the data slightly
```bash
# Replace local file urls by s3 urls in the pipeline input file
cat  data-test/data/testSample.tsv |  sed 's/data-test/s3:\/\/cqgc-qa-app-datalake/g' >testSample2.tsv
mv testSample2.tsv data-test/data/testSample.tsv 
```

## Step 2: Copy the test dataset on the minio bucket


If you haven't already, run the following command to enable access to the Minio UI:

```
kubectl port-forward svc/minio 9091:9091 -n cqgc-qa & echo $! >minikube-tmp/etl_annotate_variant/pids/minio_service_pid
```

Open your browser and go to the minio login page at `localhost:9091`. Use username `minio` and password `minio123` to login.
Using the object browser, upload the folder `minikube-tmp/etl_annotate_variant/minikube-tmp/cqdg-denovo-pipeline/data-test/data` to the bucket cqgc-qa-app-datalake. When pressing the Upload button, select the Upload Folder option.
Repeat the procedure for folder  `minikube-tmp/etl_annotate_variant/minikube-tmp/cqdg-denovo-pipeline/data-test/reference`.

The directory structure in Minio should look like this:
 - s3://cqgc-qa-app-datalake/data
 - s3://cqgc-qa-app-datalake/reference


## Step 3: Copy extra configuration files on the persistent volume

Run the following command:

```
docker cp doc/test/resources/etl_annotate_variant/params.json minikube:/mnt/cqgc-qa/nextflow/params.json
```

We previously attempted to use the minikube mount command to mount the Nextflow persistent volume. This caused permission issues when running the Nextflow job. We tried using the options --uid 0 --gid 0 (root user and group) with the minikube mount command, but the error persisted. Interestingly, Nextflow manages to create some files and folders, but fails for a specific file.


## Step 4: Run the dag

Go in the airflow UI at localhost:50080 (username=admin, password=admin) and run dag `etl_annotate_variant.py`