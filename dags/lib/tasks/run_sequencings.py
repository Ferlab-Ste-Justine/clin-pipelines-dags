from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib.config import clin_datalake_bucket, etl_run_pending_folder, s3_conn_id


@task(task_id='save_sequencing_ids_to_s3')
def save_sequencing_ids_to_s3(sequencing_ids: list) -> str:
    """
    Save sequencing IDs to S3 in the .etl-run folder for later processing by etl_run_release.
    Each sequencing ID is saved as a separate file named with the sequencing ID.
    """
    if not sequencing_ids or len(sequencing_ids) == 0:
        return "No sequencing IDs to save"
    
    s3 = S3Hook(s3_conn_id)

    for seq_id in sequencing_ids:
        # Create a file for each sequencing ID in the .etl-run folder
        s3_key = f'{etl_run_pending_folder}/{seq_id}.txt'
        # in the future we might want to save more info in the file, for now just create an empty file
        s3.load_string("", s3_key, clin_datalake_bucket, replace=True)
    
    return f"Saved {len(sequencing_ids)} sequencing IDs to S3 in .etl-run/"


@task(task_id='get_pending_sequencing_ids')
def get_pending_sequencing_ids() -> list:
    """
    Read sequencing IDs from all .txt filenames in the .etl-run folder on S3.
    """
    s3 = S3Hook(s3_conn_id)
    
    # List all files in the .etl-run folder
    keys = s3.list_keys(bucket_name=clin_datalake_bucket, prefix=f'{etl_run_pending_folder}/')
    
    if not keys:
        return []
    
    # Filter only .txt files and extract sequencing IDs from filenames
    sequencing_ids = []
    for key in keys:
        if key.endswith('.txt'):
            # Extract sequencing ID from filename: remove prefix and .txt extension
            filename = key.replace(f'{etl_run_pending_folder}/', '').replace('.txt', '')
            if filename:
                sequencing_ids.append(filename)
    
    return sequencing_ids


@task(task_id='check_pending_sequencing_ids')
def check_pending_sequencing_ids() -> str:
    """
    Check if there are any pending .txt files in the .etl-run folder on S3.
    Returns a message if files exist, raises AirflowSkipException otherwise.
    """
    s3 = S3Hook(s3_conn_id)
    
    # List all files in the .etl-run folder
    keys = s3.list_keys(bucket_name=clin_datalake_bucket, prefix=f'{etl_run_pending_folder}/')
    
    if not keys:
        raise AirflowSkipException("No pending files found in .etl-run folder")
    
    # Filter only .txt files
    txt_files = [key for key in keys if key.endswith('.txt')]
    
    if not txt_files:
        raise AirflowSkipException("No pending .txt files found in .etl-run folder")
    
    return f"Found {len(txt_files)} pending file(s) to process"


@task(task_id='cleanup_pending_sequencing_ids')
def cleanup_pending_sequencing_ids(sequencing_ids: list) -> str:
    """
    Delete only the .txt files from the .etl-run folder that were processed in this run.
    """
    if not sequencing_ids or len(sequencing_ids) == 0:
        return "No files to delete"
    
    s3 = S3Hook(s3_conn_id)
    
    for seq_id in sequencing_ids:
        # Reconstruct the filename for each processed sequencing ID
        s3_key = f'{etl_run_pending_folder}/{seq_id}.txt'
        s3.delete_objects(bucket=clin_datalake_bucket, keys=s3_key)
       
    return f"Deleted {len(sequencing_ids)} processed files from S3"
