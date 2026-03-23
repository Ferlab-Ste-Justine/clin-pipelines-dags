from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib.config import clin_datalake_bucket, etl_run_pending_folder, s3_conn_id


@task(task_id='save_run_to_s3')
def save_run_to_s3(sequencing_ids: list, batch_id: str = None) -> str:
    """
    Save sequencing IDs or batch_id to S3 in the .etl-run folder for later processing by etl_run_release.
    - Sequencing IDs are saved as: <sequencing_id>.sequencing_id (or .txt for backward compatibility)
    - Batch IDs are saved as: <batch_id>.batch_id
    """
    s3 = S3Hook(s3_conn_id)
    saved_items = []

    # Save sequencing IDs (using .sequencing_id extension, .txt still supported for reading)
    if sequencing_ids and len(sequencing_ids) > 0:
        for seq_id in sequencing_ids:
            s3_key = f'{etl_run_pending_folder}/{seq_id}.sequencing_id'
            # in the future we might want to save more info in the file, for now just create an empty file
            s3.load_string("", s3_key, clin_datalake_bucket, replace=True)
            saved_items.append(f"sequencing_id:{seq_id}")
    
    # Save batch_id
    if batch_id:
        s3_key = f'{etl_run_pending_folder}/{batch_id}.batch_id'
        s3.load_string("", s3_key, clin_datalake_bucket, replace=True)
        saved_items.append(f"batch_id:{batch_id}")
    
    return f"Saved {len(saved_items)} items to S3 in .etl-run/: {', '.join(saved_items)}"


@task(task_id='get_pending_sequencing_ids')
def get_pending_sequencing_ids() -> list:
    """
    Read sequencing IDs from .txt and .sequencing_id files in the .etl-run folder on S3.
    Returns a list of sequencing IDs (supports backward compatibility with .txt files).
    """
    s3 = S3Hook(s3_conn_id)
    
    # List all files in the .etl-run folder
    keys = s3.list_keys(bucket_name=clin_datalake_bucket, prefix=f'{etl_run_pending_folder}/')
    
    # Extract IDs from .txt (backward compatibility) and .sequencing_id files
    sequencing_ids = []
    for key in keys:
        if key.endswith('.txt'):
            # Backward compatibility: .txt files are treated as sequencing_ids
            filename = key.replace(f'{etl_run_pending_folder}/', '').replace('.txt', '')
            if filename:
                sequencing_ids.append(filename)
        elif key.endswith('.sequencing_id'):
            # Extract sequencing ID from filename
            filename = key.replace(f'{etl_run_pending_folder}/', '').replace('.sequencing_id', '')
            if filename:
                sequencing_ids.append(filename)

    return sequencing_ids


@task(task_id='get_pending_batch_ids')
def get_pending_batch_ids() -> list:
    s3 = S3Hook(s3_conn_id)
    
    # List all files in the .etl-run folder
    keys = s3.list_keys(bucket_name=clin_datalake_bucket, prefix=f'{etl_run_pending_folder}/')
    
    # Extract IDs from .batch_id files
    batch_ids = []
    for key in keys:
        if key.endswith('.batch_id'):
            # Extract batch ID from filename
            filename = key.replace(f'{etl_run_pending_folder}/', '').replace('.batch_id', '')
            if filename:
                batch_ids.append(filename)
    
    return batch_ids


@task(task_id='cleanup_pending_run_ids')
def cleanup_pending_run_ids(sequencing_ids: list = None, batch_ids: list = None) -> str:
    s3 = S3Hook(s3_conn_id)
    deleted_count = 0
    
    # Clean up sequencing IDs
    if sequencing_ids and len(sequencing_ids) > 0:
        for seq_id in sequencing_ids:
            # Try to delete all possible file types for backward compatibility
            txt_key = f'{etl_run_pending_folder}/{seq_id}.txt'
            sequencing_id_key = f'{etl_run_pending_folder}/{seq_id}.sequencing_id'
            
            # Check and delete .txt file (backward compatibility)
            if s3.check_for_key(txt_key, clin_datalake_bucket):
                s3.delete_objects(bucket=clin_datalake_bucket, keys=txt_key)
                deleted_count += 1
            # Check and delete .sequencing_id file
            elif s3.check_for_key(sequencing_id_key, clin_datalake_bucket):
                s3.delete_objects(bucket=clin_datalake_bucket, keys=sequencing_id_key)
                deleted_count += 1
    
    # Clean up batch IDs
    if batch_ids and len(batch_ids) > 0:
        for batch_id in batch_ids:
            batch_id_key = f'{etl_run_pending_folder}/{batch_id}.batch_id'
            
            # Check and delete .batch_id file
            if s3.check_for_key(batch_id_key, clin_datalake_bucket):
                s3.delete_objects(bucket=clin_datalake_bucket, keys=batch_id_key)
                deleted_count += 1
       
    return f"Deleted {deleted_count} processed files from S3"
