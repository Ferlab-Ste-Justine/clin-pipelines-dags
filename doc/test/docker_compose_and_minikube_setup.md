# Test setup with docker-compose and minikube

Here we describe a procedure to test locally with minikube. 

Airflow is still deployed with the celery executor with docker-compose as describe in the readme, but the kubernetes tasks are submitted on minikube.

So far, this procedure has been tried with minikube v1.32.0 on mac os with a naïve task. More work might be necessary to ensure that it works with other linux distributions and more complex tasks.

## Procedure

Start minikube:

```
minikube start  --embed-certs
```

Since we pass the option --embed-certs, the certificates will be included directly in the kubectl configuration file. It makes the configuration file more portable.

Create the cqgc-qa namespace on the minikube cluster:

```
kubectl create namespace cqgc-qa
```

Extract the minikube config and modify it slightly to change the hostname `127.0.0.1` to `minikubeCA`:

```
kubectl config view --minify --flatten --context=minikube | sed "s/127.0.0.1/minikubeCA/g"  >/tmp/kube_config
```

Create your .env file as instructed in the readme. Set the variable `KUBE_CONFIG` to match the config file that you created. Ex:

```
KUBE_CONFIG=/tmp/kube_config
```

In the docker-compose.yaml file, add the following lines to bloc `x-airflow-common`:

```
  extra_hosts:
      - "minikubeCA:host-gateway"
```

It will allow to redirect host `minikubeCA` to your minikube cluster. For now, if using another hostname, we see an error message telling that the hostname does not match any certificate. We tried kubernetes configuration options to change this behaviour, but they seemed to be ignored.

Run docker-compose up and create the variables in the UI, as describe in the README. Use the value `minikube` for variables
`kubernetes_context_default` and `kubernetes_context_etl`


You are ready to test a simple dag. You can try dags `test_pod_operator_etl` or `test_pod_operator_default`.
