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

From the minio UI, create buckets with name `cqgc-qa-app-files-import` and `cqgc-qa-app-files-scratch`

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
            "Effect": "Allow",
            "Action": [
                "s3:Get*",
                "s3:List*",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::cqgc-qa-app-files-scratch*",
                "arn:aws:s3:::cqgc-qa-app-files-import*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::cqgc-qa-app-files-scratch*"
            ]
        }
    ]
}
```
Note: You can use any policy you'd like to test locally. However, we recommend using a policy as close as possible to the production environment to avoid unanticipated permission issues when deploying.


## Optional: save minio state


At the end of the procedure, you can optionally save a copy of internal Minio data files. These can be used to restore the Minio state later.
Simply run the following command:

```
docker cp minikube:/mnt/cqgc-qa/minio/data minikube-tmp/etl_annotate_variant/minio-data
```

Note:
We chose to not use the `minikube mount` command to copy Minio state files because it causes the Minio pod to crash due to a permission issue. It might be worth trying again with the additional options `--uid 0 --gid 0` when running the minikube mount command. This should set the owner and group to root for the mounted folder in the minikube container.
