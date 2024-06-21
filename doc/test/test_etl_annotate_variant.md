# Test etl_annotate_variant DAG

Here we describe how to test etl_annotate_variant DAG locally, with minikube.

## Procedure

Make a basic setup with minikube. Choose one of the following procedures:
- [hybrid_setup_docker_compose_and_minikube](./hybrid_setup_docker_compose_and_minikube.md)
- [minikube_setup](./minikube_setup.md)

Deploy minio:

```
kubectl create -f doc/test/templates/mock_minio.yaml -n cqgc-qa
```


Mount minio persistent volume:

```
minikube mount ./data:/mnt/cqgc-qa/minio/data
````

To be able to access the minio UI:

```
kubectl port-forward svc/minio 9091:9091 -n cqgc-qa
```



Go to the minio login page and login with username and password `minioadmin`:

```
http://localhost:9091/

```

Create a bucket with name `cqgc-qa-app-datalake`

Create access key and secret key pair. Configure the following policy on it:

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


Store the s3 credentials in a kubernetes secret:

```
kubectl create secret generic download-s3-credentials --from-literal=S3_ACCESS_KEY=VJbTFea2n35Wbe3CNsZH --from-literal=S3_SECRET_KEY=n0Tuyz3zw1NIKh9LBsUXzULjJpRhkeauaqcKFyK5 -n cqgc-qa
```

Create nextflow resources:

```
kubectl create -f doc/test/templates/nextflow.yaml -f doc/test/templates/nextflow_config.yaml -n cqgc-qa
```

Go in the airflow UI ()  and run dag `etl_annotate_variants`