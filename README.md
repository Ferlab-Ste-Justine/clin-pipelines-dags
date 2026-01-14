# CLIN Pipelines Dags

## Python virtual environment
The code work with ```Python 3.12.11```.
> Info : You can use [pyenv](https://github.com/pyenv/pyenv) to manage Python versions

Create venv :

```zsh
python -m venv .venv
```

Activate venv :

```zsh
source .venv/bin/activate
```

Install requirements :

```zsh
pip install -r requirements.txt
```

---
## Run tests
Run all tests :
```bash
pytest
```

Run all tests except slow tests:
```bash
pytest -m "not slow"
```

Run all tests except tests that require VPN connection:
```bash
pytest -m "not vpn"
```
---

## Airflow dev stack
Configuration files of containers are in: ```configs/```.  
All datas (Airflow logs, Minio files, postgres data) are in: ```data/```.

1. Create `.env` file :
```zsh
make setup
```

1. Create the minio buckets :
```zsh
make create_buckets
```

3. Deploy stack :
```zsh
make start
```

> You can still use the old compose file:  
>```zsh
> docker-compose up
>```
> (Airflow logs will be in the ```logs/``` folder)

## Airflow dev stack (without minio)
If you are using the clin-localstack project, or an existing Minio instance, the Minio instance can be used as S3 solution for this local setup.  

Configuration files of containers are in: ```configs/```.  
All datas (Airflow logs, postgres data) are in: ```data/```.

1. Create the .env file
```zsh
make setup_localstack
```
2. Start containers
```zsh
make start_localstack
```

## Set Airflow variables and connections

### Set variables
Login to Airflow UI :

- URL : `http://localhost:50080`
- Username : `airflow`
- Password : `airflow`

Import Airflow variables:
1. Airflow UI => Admin => Variables
2. Upload file: `variables.json`. 

Variables should be:
- environment : `qa`
- kubernetes_namespace : `cqgc-qa`
- kubernetes_context_default : `kubernetes-admin-cluster.qa.cqgc@cluster.qa.cqgc`
- kubernetes_context_etl : `kubernetes-admin-cluster.etl.cqgc@cluster.etl.cqgc`
- base_url (optional) : `http://localhost:50080`
- show_test_dags (optional) : `yes`
- s3_conn_id : `minio`

Test one task :

```zsh
docker-compose exec airflow-scheduler airflow tasks test <dag> <task> 2022-01-01
```

### Set connections
Login to Airflow UI :

- URL : `http://localhost:50080`
- Username : `airflow`
- Password : `airflow`

#### MinIO

Create Airflow connection:
1. Airflow UI => Admin => Connections
2. Set connection data: 
- Connection Id : `minio`
- Connection Type : `Amazon Web Services`
- Extra :
```json
// For default setup
{
  "endpoint_url": "http://minio:9000",
  "verify": false,
  "aws_access_key_id": "minioadmin",
  "aws_secret_access_key": "minioadmin"
}
// For external minio setup
{
  "endpoint_url": "http://minio-external:9000",
  "verify": false,
  "aws_access_key_id": "<your minio access key>",
  "aws_secret_access_key": "<your minio password key>"
}
```

#### gnomAD
1. Airflow UI => Admin => Connections
2. Set connection data: 
- Connection Id : `gnomad`
- Connection Type : `Amazon Web Services`
- Extra :
```json
{
  "config_kwargs": {
    "signature_version": "unsigned"
  }
}
```

## Troubleshooting

### Failed to establish a new connection: [Errno 110] Connection timed out

Can be a host <=> ip resolution issue in local. Add to your `/etc/hosts` file the following :

```
10.128.81.22  k8-api.etl.cqgc.hsj.rtss.qc.ca
10.128.81.202 k8-api.qa.cqgc.hsj.rtss.qc.ca
```
