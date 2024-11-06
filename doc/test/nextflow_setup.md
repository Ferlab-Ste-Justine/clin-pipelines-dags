# Nextflow setup

This guide describes the local minikube setup procedure common to most DAGs based on the nextflow operator. Additional steps or tweaks may be required depending on the specific DAG.

## Procedure

1. **Install and configure airflow on minikube** 

    Choose one of the following procedures:
    - [Docker Compose and Minikube Setup](./docker_compose_and_minikube_setup.md)
    - [Minikube Setup](./minikube_setup.md)

2. **Install and setup minio**

    We use an S3 bucket (minio) to store intermediate files when running nextflow jobs. Here, 
    we start a local minio service and create the necessary buckets.

    First create required storage resources, i.e. a persistent volume and the associated claim.
    ```
    kubectl apply -f doc/test/templates/nextflow/minio_pv.yaml -n cqgc-qa
    ```

    Then create minio pod(s) and service(s):
    ```
    kubectl apply -f doc/test/templates/nextflow/mock_minio.yaml -n cqgc-qa
    ```

    Run the following command to access the minio UI:
    ```
    mkdir -p doc/test/.minikube-setup/nextflow/pids
    kubectl port-forward svc/minio 9091:9091 -n cqgc-qa & echo $! >doc/test/.minikube-setup/nextflow/pids/minio_service_pid
    ```

    Login with username `minio` and password `minio123`. Through the UI, create buckets `cqgc-qa-app-files-import` and `cqgc-qa-app-files-scratch`.

    By default, we assume that you are ok with using the minio admin credentials later to access minio and manipulate these buckets. If this is not acceptable, you can create the desired credentials and permissions through the UI as well. Make sure to save a copy of the created credentials, as we will need to inject them as a secret in the minikube cluster.

4. **Create minio credentials secrets**

This will inject minio admin credentials in a kubernetes secret that will be used to inject the credentials on the nextflow pod:
```
 kubectl create secret generic cqgc-qa-minio-app-nextflow --from-literal=access_key=minio --from-literal=secret_key=minio123 -n cqgc-qa
```

If you created credentials at step 2, replace `minio` and `minio123` values above by the access and secret keys that you created.


5. **Create other required nextflow resources**

    ```bash
    # Create config map that will be used to inject a configuration file in the pod
    kubectl apply -f doc/test/templates/nextflow/nextflow_config.yaml -n cqgc-qa

    # Create dedicated persistent volume, persistent volume claim and service account resources:
    kubectl apply -f doc/test/templates/nextflow/nextflow.yaml -n cqgc-qa

    # Make sure that the storage path associated to the persistent volume exists on the minikube container:    
    docker exec -it minikube mkdir -p /mnt/cqgc-qa/nextflow
    ```



6. **Test**

    You are now ready to execute a simple nextflow operator. Go to the airflow ui (localhost:50080), and run the dag `test_nextflow_operator.py`. 

    You can use the following command to list the pods running in the minikube cluster:
    `kubectl get pod -n cqgc-qa`