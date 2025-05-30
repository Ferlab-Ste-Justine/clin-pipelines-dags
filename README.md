# CLIN Pipelines Dags

## Python virtual environment

Create venv :

```
python -m venv .venv
```

Activate venv :

```
source .venv/bin/activate
```

Install requirements :

```
pip install -r requirements.txt
```

## Airflow dev stack

Create `.env` file :

```
cp .env.sample .env
```

Deploy stack :

```
docker-compose up
```

Login to Airflow UI :

- URL : `http://localhost:50080`
- Username : `airflow`
- Password : `airflow`

Create Airflow variables (Airflow UI => Admin => Variables) :

- environment : `qa`
- kubernetes_namespace : `cqgc-qa`
- kubernetes_context_default : `kubernetes-admin-cluster.qa.cqgc@cluster.qa.cqgc`
- kubernetes_context_etl : `kubernetes-admin-cluster.etl.cqgc@cluster.etl.cqgc`
- base_url (optional) : `http://localhost:50080`
- show_test_dags (optional) : `yes`

_For faster variable creation, upload the `variables.json` file in the Variables page._

Test one task :

```
docker-compose exec airflow-scheduler airflow tasks test <dag> <task> 2022-01-01
```

## MinIO

Login to MinIO console :

- URL : `http://localhost:59001`
- Username : `minioadmin`
- Password : `minioadmin`

Create Airflow variable (Airflow UI => Admin => Variables) :

- s3_conn_id : `minio`

Create Airflow connection (Airflow UI => Admin => Connections) :

- Connection Id : `minio`
- Connection Type : `Amazon S3`
- Extra :

```
{
    "host": "http://minio:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin"
}
```

- Connection Id : `gnomad`
- Connection Type : `Amazon S3`
- Extra :

```
{
    "config_kwargs": {
      "signature_version": "unsigned"
    }
}
```

## Slack

Create Airflow variable (Airflow UI => Admin => Variables) :

- slack_hook_url : `https://hooks.slack.com/services/...`

## Troubleshooting

### Failed to establish a new connection: [Errno 110] Connection timed out

Can be a host <=> ip resolution issue in local. Add to your `/etc/hosts` file the following :

```
10.128.81.22  k8-api.etl.cqgc.hsj.rtss.qc.ca
10.128.81.202 k8-api.qa.cqgc.hsj.rtss.qc.ca
```
