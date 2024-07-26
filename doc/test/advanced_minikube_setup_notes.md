# Advanced Minikube Setup Notes

This document serves as a collection of notes for experimenting with more intricate setups involving minikube and airflow. 

It aims to document the installation steps and configurations used when deviating from the standard setup or testing a more complex dag.

## Use kubeconfig file instead in-cluster mode

If you would like to use the kubeconfig file instead the in-cluster mode for operators connecting to the Kubernetes cluster, you will need to tweak the setup procedure. 

Use the following command to start minikube:

```
minikube start  --embed-certs --apiserver-names=host.minikube.internal --memory=max --cpus=max
```

Before installing airflow with helm, run the following commands:

```
kubectl config view --minify --flatten --context=minikube | sed "s/127.0.0.1/host.minikube.internal/g"  >/tmp/kube_config
kubectl create secret generic cqgc-qa-airflow-k8-client-config --from-file=config=/tmp/kube_config -n cqgc-qa
```

When installing airflow with helm, use file [values_with_kubeconfig_file.yaml](templates/airflow/values_with_kubeconfig_file.yaml) instead [values.yaml](templates/airflow/values.yaml). I.e.: 

```
helm upgrade --install airflow apache-airflow/airflow --version 1.11.0 --namespace cqgc-qa --values doc/test/templates/airflow/values_with_kubeconfig_file.yaml
```