# Test setup with minikube

This document describes a procedure to test the airflow ETL locally with minikube, where all components, including airflow, will run as pods in a minikube cluster.

This setup is particularly useful if you'd like to locally test an operator extending the `KubernetesPodOperator`. Alternatively, you can deploy airflow via docker compose and use the minikube cluster only for operators running as kubernetes pods. If you prefer this setup, see `docker_compose_and_minikube_setup.md`.

To simplify the installation procedure, we use the in-cluster mode to connect to the Kubernetes cluster for operators based on the KubernetesPodOperator class. This means that cluster connection settings will be retrieved from the cluster in which airflow is deployed (i.e. minikube here).

Unlike the docker-compose setup, we are not using minio to store airflow logs. We simply use a persistent volume created automatically on the minikube container.

So far, this procedure has been tried with minikube v1.32.0 on mac os with a naïve task. More work might be necessary to ensure that it works with other linux distributions and more complex tasks.


## Prerequisites

- docker
- kubectl
- minikube
- helm


## Procedure

First create the minikube cluster.

```
minikube start  --memory=max --cpus=max
```

Create the cqgc-qa namespace:

```
kubectl create namespace cqgc-qa
```

Install persistent volume and associated claim for dags:

```
kubectl apply -f doc/test/templates/airflow/airflow.yaml -n cqgc-qa
```


Mount the `dags`folder in the persistent volume.  The following command will start the minikube mount process in the background and store the PID in the file `dags_mount_pid`. To kill the process, use `kill <pid>` with the stored PID.
```
mkdir -p doc/test/.minikube-setup/pids
minikube mount ./dags:/mnt/cqgc-qa/airflow/dags & echo $! >doc/test/.minikube-setup/pids/dags_mount_pid
```

Install airflow with helm:

```
# Add the airflow helm repository. This only need to be done once.
helm repo add apache-airflow https://airflow.apache.org

# Then install airflow in the minikube cluster:
helm upgrade --install airflow apache-airflow/airflow --version 1.15.0 --namespace cqgc-qa --values doc/test/templates/airflow/values.yaml
```

Wait that all pods are ready and use the following command to access the airflow UI:

```
kubectl port-forward svc/airflow-webserver 50080:8080 --namespace cqgc-qa & echo $! >doc/test/.minikube-setup/pids/airflow_server_pid
```

Open your browser and go to the airflow login page at localhost:50080. You can enter `admin` as username and password.

You can run dags `test_pod_operator_default` and `test_pod_operator_etl` to check that all is ok.

The airflow variables, typically created via the Airflow UI (Admin => Variables), should be pre-created. These variables will not appear in the UI. You can view the configured values in the `test/templates/airflow/values.yaml` file (see the `env` section). If necessary, you can overwrite these values via the Airflow UI.


## Clean up

You can simply run the command `minikube delete` to destroy the minikube cluster. 