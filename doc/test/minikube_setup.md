# Test setup with minikube

Here we describe a procedure to test the airflow etl locally with minikube. 

All components, including airflow, will run as pods in a minikube cluster.

So far, this procedure has been tried with minikube v1.32.0 on mac os with a naïve task. More work might be necessary to ensure that 
it works with other linux distributions and more complex tasks.


## Prerequisites

- kubectl
- minikube
- helm

## Procedure

First create the minikube cluster:

```
minikube start  --embed-certs --apiserver-names=host.minikube.internal
```

Create the cqgc-qa namespace:

```
kubectl create namespace cqgc-qa
```

Create persistent volume and associated claim for dags:

```
kubectl create -f doc/test/templates/pv_dags.yaml -f doc/test/templates/pvc_dags.yaml -n cqgc-qa
```

The tasks based on the KubernetesPodOperator requires a kubeconfig file. We extract minikube
configuration and store it in a secret. We use host `host.minikube.internal`
instead 127.0.0.1, as recommended in minikube documentation:
https://minikube.sigs.k8s.io/docs/handbook/host-access/

```
kubectl config view --minify --flatten --context=minikube | sed "s/127.0.0.1/host.minikube.internal/g"  >/tmp/kube_config
kubectl create secret generic cqgc-qa-airflow-k8-client-config --from-file=config=/tmp/kube_config -n cqgc-qa
```

Mount the dags folder in the dags persistent volume. Run this in another shell. Make sure that you are at
the clin-pipeline-dags project root:

```
minikube mount ./dags:/mnt/cqgc-qa/airflow/dags
```


Install airflow components via helm. You will need to install the helm client first.

```
helm upgrade --install airflow apache-airflow/airflow --version 1.11.0 --namespace cqgc-qa --values doc/test/templates/airflow_values.yaml
```

Wait that all pods are ready and use the following command to access the airflow UI:

```
kubectl port-forward svc/airflow-webserver 50080:8080 --namespace cqgc-qa
```

You can enter `admin` as username and password.