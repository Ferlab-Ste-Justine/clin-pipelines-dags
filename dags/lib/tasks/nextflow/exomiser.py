from typing import Dict, Set

from airflow.decorators import task

from lib.datasets import enriched_clinical
from lib.utils import SKIP_EXIT_CODE


@task.virtualenv(skip_on_exit_code=SKIP_EXIT_CODE, task_id='prepare_exomiser', requirements=["deltalake===0.24.0", "phenopackets===2.0.2.post4"],
                 inlets=[enriched_clinical])
def prepare(analysis_ids: Set[str], skip: str) -> Dict[str, str]:
    """
    Prepare phenopacket files for nextflow exomiser run.

    For each proband in the given analysis IDs, construct a phenopacket file containing its phenotypic features and
    family information, and upload it to S3.
    :param analysis_ids: Input analysis IDs
    :return: A mapping of analysis IDs to the S3 path of the corresponding phenopacket file
    """
    import logging
    import sys

    import pandas as pd
    from pandas import DataFrame

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from google.protobuf.json_format import MessageToJson
    from phenopackets.schema.v1 import base_pb2 as pp_base
    from phenopackets.schema.v1 import phenopackets_pb2 as pp

    from lib.config import s3_conn_id
    from lib.config_nextflow import nextflow_bucket, nextflow_exomiser_input_key
    from lib.datasets import enriched_clinical
    from lib.utils import SKIP_EXIT_CODE
    from lib.utils_etl_tables import to_pandas

    if skip:
        sys.exit(SKIP_EXIT_CODE)

    s3 = S3Hook(s3_conn_id)

    df: DataFrame = to_pandas(enriched_clinical.uri)

    filtered_df = df[df['analysis_id'].isin(analysis_ids)]
    clinical_df = filtered_df[['analysis_id', 'aliquot_id', 'gender',
                               'clinical_signs', 'is_proband', 'family_id', 'father_aliquot_id', 'mother_aliquot_id',
                               'affected_status_code']]
    probands_df = clinical_df[clinical_df['is_proband']]

    # Mapping enriched_clinical gender to phenopacket sex enum
    sex_mapping: Dict[str, pp_base.Sex] = {
        'Male': pp_base.MALE,
        'Female': pp_base.FEMALE,
        'unknown': pp_base.UNKNOWN_SEX,
        'other': pp_base.OTHER_SEX
    }

    # Mapping enriched_clinical affected_status_code to phenopacket affected status enum
    affected_mapping: Dict[str, pp_base.Pedigree.Person.AffectedStatus] = {
        "affected": pp_base.Pedigree.Person.AFFECTED,
        "not_affected": pp_base.Pedigree.Person.UNAFFECTED,
        "unknown": pp_base.Pedigree.Person.MISSING
    }

    analysis_file_mapping = {}

    for _, proband_row in probands_df.iterrows():
        # Construct proband.subject field
        proband = pp_base.Individual(
            id=proband_row['aliquot_id'],
            sex=sex_mapping.get(proband_row['gender']),
        )

        # Construct proband.phenotypicFeatures field
        clinical_signs = proband_row['clinical_signs']
        phenotypic_features = [pp_base.PhenotypicFeature(
            type=pp_base.OntologyClass(id=sign["id"], label=sign["name"])
        ) for sign in clinical_signs if sign["affected_status"]]

        # Construct pedigree.persons field
        analysis_id = proband_row['analysis_id']
        family_df = clinical_df[clinical_df['analysis_id'] == analysis_id]
        persons = [pp_base.Pedigree.Person(
            family_id=analysis_id,
            individual_id=person['aliquot_id'],
            sex=sex_mapping.get(person['gender']),
            affected_status=affected_mapping.get(person['affected_status_code']),
            paternal_id=person['father_aliquot_id'] if pd.notna(person['father_aliquot_id']) else None,
            maternal_id=person['mother_aliquot_id'] if pd.notna(person['mother_aliquot_id']) else None
        ) for _, person in family_df.iterrows()]

        pedigree = pp_base.Pedigree(
            persons=persons
        )

        # Create the phenopacket
        phenopacket = pp.Phenopacket(
            subject=proband,
            phenotypic_features=phenotypic_features
        )
        family = pp.Family(
            id=analysis_id,
            proband=phenopacket,
            pedigree=pedigree
        )

        # Upload phenopacket json file to S3
        json_data = MessageToJson(family)
        s3_key = nextflow_exomiser_input_key(analysis_id)
        s3.load_string(
            string_data=json_data,
            key=s3_key,
            bucket_name=nextflow_bucket,
            replace=True
        )
        file_path = f"s3://{nextflow_bucket}/{s3_key}"
        logging.info(f"Phenopacket file for analysis {analysis_id} uploaded to S3 path: {file_path}")

        analysis_file_mapping[analysis_id] = file_path

    return analysis_file_mapping
