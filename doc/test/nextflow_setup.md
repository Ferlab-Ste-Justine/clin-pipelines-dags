# Nextflow setup

This guide describes the Minikube setup procedure common to most DAGs based on the Nextflow operator. Additional steps or tweaks may be required depending on the specific DAG.

## Procedure

1. **Install and configure airflow on minikube** 

    Choose one of the following procedures:
    - [Docker Compose and Minikube Setup](./docker_compose_and_minikube_setup.md)
    - [Minikube Setup](./minikube_setup.md)

2. **Create storage resources for minio**
    We use an S3 bucket to store intermediate files when using mextflow.  Minio will be used to mock S3 locally.

    First create required storage resources, i.e. a persistent volume and the associated claim.
    ```
    kubectl apply -f doc/test/templates/nextflow/minio_pv.yaml -n cqgc-qa
    ```

3. **Install and Configure Minio**

    Follow the steps documented in [nextflow_minio_setup.md](./nextflow_minio_setup.md) to install and configure minio. Alternatively, if you have saved minio state files as instructed, you can restore minio by performing the following steps:

   ```bash

   # Copy the internal Minio data files to the persistent volume:
   docker cp minikube-tmp/etl_annotate_variant/minio-data minikube:/mnt/cqgc-qa/minio/data

   # Install minio
    kubectl apply -f doc/test/templates/nextflow/mock_minio.yaml -n cqgc-qa
   ```

   Note that we did not use the Minikube mount command to copy Minio state files. This is because it causes the minio pod to crash.

4. **(Optional) Verify minio setup**

    To access the minio UI and verify the setup, run the following command:

    ```
    kubectl port-forward svc/minio 9091:9091 -n cqgc-qa & echo $! >minikube-tmp/etl_annotate_variant/pids/minio_service_pid
    ```

    Then, open your web browser and go to the minio login page. Login with username `minio` and password `minio123`:

    ```
    http://localhost:9091/

    ```

    Sometimes, the login may not work on the first attempt. If this happens, terminate the port forwarding process and repeat the steps above.

    In the UI, you should see a bucket named `cqgc-qa-app-datalake` and credentials named `nextflow-test` with the appropriate policy configured.

5. **Create required nextflow resources**

    ```bash
    # Store S3 credentials as secret. These will be injected as environment variables in the nextflow pod
    kubectl create secret generic download-s3-credentials --from-literal=S3_ACCESS_KEY=nextflow-test --from-literal=S3_SECRET_KEY=nextflow-test -n cqgc-qa

    # Create config map that will be used to inject a configuration file in the pod
    kubectl apply -f doc/test/templates/nextflow/nextflow_config.yaml -n cqgc-qa

    # Create dedicated persistent volume, persistent volume claim and service account resources:
    kubectl apply -f doc/test/templates/nextflow/nextflow.yaml -n cqgc-qa
    ```

6. **Test**

    You are now ready to execute a simple nextflow operator. Go to the airflow ui (localhost:50080, username=`admin`, password=`admin`), and run the dag `test_nextflow_operator.py`. 