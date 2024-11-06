# Test setup with docker-compose and minikube

This document describes how to use the KubernetesPodOperator with minikube for local testing.

Airflow is still deployed with the celery executor with docker-compose as describe in the readme. The KubernetesPodOperator tasks are submitted to a minikube kubernetes cluster running locally.

If you would prefer to deploy the full ETL, including airflow, in the minikube cluster, see `minikube_setup.md`.

So far, this procedure has been tried with minikube v1.32.0 on mac os with a naïve task. More work might be necessary to ensure that it works with other linux distributions and more complex tasks.

## Procedure

Start minikube:
```
minikube start  --embed-certs  --apiserver-names=host.docker.internal  --memory=max --cpus=max 
```

Since we pass the `--embed-certs` option, the certificates will be included directly in the `kubectl` configuration file. This is important for the next steps. The  `--apiserver-names=host.docker.internal` option will allow 
docker containers to interact with the cluster.

You might want to use more restrictive values for the memory and CPU options. The default values might not be sufficient, as the minikube cluster will operate within a single Docker container. Insufficient memory and CPU resources for this container can lead to unstable behavior.

Create the cqgc-qa namespace on the minikube cluster:
```
kubectl create namespace cqgc-qa
```

Extract the minikube config and modify it slightly to change the hostname `127.0.0.1` to `host.docker.internal`:
```
mkdir -p doc/test/.minikube-setup
kubectl config view --minify --flatten --context=minikube | sed "s/127.0.0.1/host.docker.internal/g"  >doc/test/.minikube-setup/kube_config
```

Note that, when creating the minikube cluster with the `minikube start` command, minikube will generate new certificates by default. You will need to re-run this command if you delete and re-create the minikube cluster.

Create your .env file as instructed in the readme. Set the variable `KUBE_CONFIG` to match the config file that you created:
```
KUBE_CONFIG=./doc/test/.minikube-setup/kube_config
```

Run `docker-compose up -d` and create the variables in the UI, as describe in the README. Use the value `minikube` for variables
`kubernetes_context_default` and `kubernetes_context_etl`


You are ready to test a simple dag. You can try dags `test_pod_operator_etl` or `test_pod_operator_default`.


## Clean up

You can simply run the command `minikube delete` to destroy the minikube cluster.
To shut down airflow, you can run `docker-compose down`