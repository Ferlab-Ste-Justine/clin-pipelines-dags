from airflow import Dataset

from lib.config import env

enriched_clinical = Dataset(f"s3a://cqgc-{env}-app-datalake/enriched/clinical")
