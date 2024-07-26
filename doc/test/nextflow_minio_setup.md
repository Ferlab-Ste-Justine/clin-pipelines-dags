# Install and configure minio from scratch (nextflow dags)

To enable nextflow to store intermediate files on S3, we need to install and configure minio.

This document provides step-by-step instructions for setting up and configuring minio from scratch for nextflow. Specifically, you will:
- Install minio
- Create a bucket named `cqgc-qa-app-datalake`
- Create credentials (access and secret keys) and configure associated permissions

Optionally, you can save a copy of internal minio data files to restore the minio state later.


## Procedure ##

### Prerequisites

Execute steps 1 and 2 in (nextflow_setup.md)[nextflow_setup.md] procedure.

It consists in setting up airflow and creating the storage resources (pv and pvc) for minio.


## Step 2: Installation

You can use the following command to setup the minio pod and service:

```
kubectl apply -f doc/test/templates/nextflow/mock_minio.yaml -n cqgc-qa
```

## Step 3: Login to minio

Run this command be able to access the minio UI:

```
kubectl port-forward svc/minio 9091:9091 -n cqgc-qa & echo $! >minikube-tmp/etl_annotate_variant/pids/minio_service_pid
```


Go to the minio login page and login with username `minio` and password `minio123`:

```
http://localhost:9091/

```

## Step 4: Create bucket, credentials and permissions

From the minio UI, create a bucket with name `cqgc-qa-app-datalake`

Then create an access key with the following details:
 - Access Key: `nextflow-test`
 - Secret Key: `nextflow-test

Create an access key. Use `nextflow-test` for the access and secret keys. 

Configure the following policy on the access key:

```
{
 "Version": "2012-10-17",
 "Statement": [
  {
   "Sid": "BucketAccessForUser",
   "Effect": "Allow",
   "Action": [
    "s3:GetObject",
    "s3:ListBucket",
    "s3:PutObject",
    "s3:DeleteObject"
   ],
   "Resource": [
    "arn:aws:s3:::*"
   ]
  }
 ]
}
```

## Optional: save minio state


At the end of the procedure, you can optionally save a copy of internal Minio data files. These can be used to restore the Minio state later.
Simply run the following command:

```
docker cp minikube:/mnt/cqgc-qa/minio/data minikube-tmp/etl_annotate_variant/minio-data
```

Please note that we avoid using the `minikube mount`command to sync with the persistent volume as it causes the minio pod to crash.