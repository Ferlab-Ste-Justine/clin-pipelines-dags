# Setup to test etl_annotate_variant dag

This guide describes the steps to set up and run the `etl_annotate_variant` DAG locally using Minikube.


## Prerequisites

Follow the procedure in [nextflow_setup.md](nextflow_setup.md)

## Step 1: Download and prepare a test dataset

This only needs to be done once.

Run the following to download a public test dataset on your machine:

```bash
# Download test data
mkdir -p minikube-tmp/etl_annotate_variant
cd minikube-tmp/etl_annotate_variant
git clone  --branch feat/add-test-dataset --depth 1 --no-checkout git@github.com:Ferlab-Ste-Justine/cqdg-denovo-pipeline.git
cd cqdg-denovo-pipeline
git sparse-checkout set --no-cone data-test
git checkout 

# Replace local file urls by s3 urls in the pipeline input file
cat  data-test/data/testSample.tsv |  sed 's/data-test/s3:\/\/cqgc-qa-app-datalake/g' >testSample2.tsv
mv testSample2.tsv data-test/data/testSample.tsv 
 cd ../../..
```

The test dataset will be downloaded to the folder `minikube-tmp/etl_annotate_variant/minikube-tmp/cqdg-denovo-pipeline/data-test`

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


## Step 3: Mount the persistent volume and copy extra configuration files

Run the following commands:

```bash
# Optional: to erase previous nextflow files:
rm -rf minikube-tmp/etl_annotate_variant/nextflow-workspace

# setup the minikube mount
mkdir -p minikube-tmp/etl_annotate_variant/nextflow-workspace
minikube mount minikube-tmp/etl_annotate_variant/nextflow-workspace:/mnt/cqgc-qa/nextflow & echo $! >minikube-tmp/etl_annotate_variant/pids/nextflow_workspace_mount_pid

# Copy configuration files
cp doc/test/resources/etl_annotate_variant/params.json  minikube-tmp/etl_annotate_variant/nextflow-workspace
cp doc/test/resources/etl_annotate_variant/test.config  minikube-tmp/etl_annotate_variant/nextflow-workspace
```

## Step 4: Run the dag

Go in the airflow UI at localhost:50080 (username=admin, password=admin) and run dag `etl_annotate_variant.py`