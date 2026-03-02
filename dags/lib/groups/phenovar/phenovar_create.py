import logging
from typing import List

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lib import config
from lib.datasets import enriched_clinical
from lib.phenovar import (
    PhenotypingStatus, build_phenovar_payload, can_submit_analysis,
    copy_vcf_to_phenovar_bucket, extract_vcf_filename,
    get_clinical_data, map_phenovar_file_type, parse_s3_url,
    submit_phenovar_analysis, write_s3_analysis_status
)


@task_group(group_id='create')
def phenovar_create(analysis_ids: List[str], skip: str):
    """
    Task group for creating analyses in Phenovar API.
    
    Steps:
    1. Extract clinical data (patient info, HPO codes, VCF URLs)
    2. Copy VCF files to Phenovar import bucket
    3. Submit analysis requests to Phenovar API
    """
    
    @task.virtualenv(requirements=["deltalake===0.24.0"], inlets=[enriched_clinical])
    def extract_clinical_data(_analysis_ids: List[str], _skip: str) -> List[dict]:
        """
        Extract clinical data from enriched_clinical table for each analysis.
        Returns list of dicts with patient info, HPO codes, and VCF URLs.
        """
        if _skip:
            raise AirflowSkipException()
        
        from lib.phenovar import get_clinical_data
        
        clinical_data = get_clinical_data(_analysis_ids)
        logging.info(f'Extracted clinical data for {len(clinical_data)} records')
        
        # Group by analysis_id to handle trios (multiple rows per analysis)
        analysis_groups = {}
        for row in clinical_data:
            analysis_id = row['analysis_id']
            if analysis_id not in analysis_groups:
                analysis_groups[analysis_id] = []
            analysis_groups[analysis_id].append(row)
        
        logging.info(f'Grouped into {len(analysis_groups)} analyses')
        return analysis_groups
    
    @task
    def copy_vcfs(analysis_groups: dict, _skip: str) -> dict:
        """
        Copy VCF files from download bucket to Phenovar import bucket.
        Returns dict mapping analysis_id to list of VCF file metadata.
        """
        if _skip:
            raise AirflowSkipException()
        
        clin_s3 = S3Hook(config.s3_conn_id)
        phenovar_s3 = S3Hook(config.s3_conn_id)  # Same connection for all buckets
        
        analysis_vcf_files = {}
        
        for analysis_id, rows in analysis_groups.items():
            vcf_files = []
            
            # Find proband row to get family relationship info
            proband_row = next((r for r in rows if r['is_proband']), None)
            if not proband_row:
                logging.warning(f'No proband found for analysis {analysis_id}, skipping')
                continue
            
            father_aliquot_id = proband_row.get('father_aliquot_id')
            mother_aliquot_id = proband_row.get('mother_aliquot_id')
            
            for row in rows:
                aliquot_id = row['aliquot_id']
                is_proband = row['is_proband']
                is_mother = aliquot_id == mother_aliquot_id
                is_father = aliquot_id == father_aliquot_id
                
                # Process SNV VCF
                snv_urls = row.get('snv_vcf_germline_urls')
                if snv_urls:
                    # Parse WrappedArray format
                    snv_url_str = str(snv_urls)
                    if snv_url_str and snv_url_str != 'None':
                        try:
                            source_bucket, source_key = parse_s3_url(snv_url_str)
                            filename = extract_vcf_filename(source_key)
                            
                            copy_vcf_to_phenovar_bucket(
                                clin_s3, phenovar_s3, analysis_id,
                                source_bucket, source_key, filename
                            )
                            
                            vcf_files.append({
                                'filepath': filename,
                                'filetype': map_phenovar_file_type(is_proband, is_mother, is_father, is_snv=True)
                            })
                        except Exception as e:
                            logging.error(f'Error processing SNV VCF for {aliquot_id}: {str(e)}')
                
                # Process CNV VCF (only for proband in initial implementation)
                cnv_urls = row.get('cnv_vcf_germline_urls')
                if cnv_urls and is_proband:
                    cnv_url_str = str(cnv_urls)
                    if cnv_url_str and cnv_url_str != 'None':
                        try:
                            source_bucket, source_key = parse_s3_url(cnv_url_str)
                            filename = extract_vcf_filename(source_key)
                            
                            copy_vcf_to_phenovar_bucket(
                                clin_s3, phenovar_s3, analysis_id,
                                source_bucket, source_key, filename
                            )
                            
                            vcf_files.append({
                                'filepath': filename,
                                'filetype': map_phenovar_file_type(is_proband, is_mother, is_father, is_snv=False)
                            })
                        except Exception as e:
                            logging.error(f'Error processing CNV VCF for {aliquot_id}: {str(e)}')
            
            analysis_vcf_files[analysis_id] = vcf_files
            logging.info(f'Copied {len(vcf_files)} VCF files for analysis {analysis_id}')
        
        return {
            'analysis_groups': analysis_groups,
            'analysis_vcf_files': analysis_vcf_files
        }
    
    @task
    def submit_analyses(data: dict, _skip: str) -> List[str]:
        """
        Build payload and submit analysis requests to Phenovar API.
        Returns list of submitted analysis IDs.
        """
        if _skip:
            raise AirflowSkipException()
        
        analysis_groups = data['analysis_groups']
        analysis_vcf_files = data['analysis_vcf_files']
        
        clin_s3 = S3Hook(config.s3_conn_id)
        submitted_ids = []
        
        for analysis_id, rows in analysis_groups.items():
            # Check if we can submit (idempotency check)
            if not can_submit_analysis(clin_s3, analysis_id):
                logging.info(f'Analysis {analysis_id} already submitted or completed, skipping')
                continue
            
            # Find proband row for main patient details
            proband_row = next((r for r in rows if r['is_proband']), None)
            if not proband_row:
                logging.warning(f'No proband found for analysis {analysis_id}, skipping')
                continue
            
            # Determine maternal/paternal affected status
            mother_aliquot_id = proband_row.get('mother_aliquot_id')
            father_aliquot_id = proband_row.get('father_aliquot_id')
            
            maternal_affected = False
            paternal_affected = False
            
            for row in rows:
                if row['aliquot_id'] == mother_aliquot_id:
                    maternal_affected = row.get('affected_status', False)
                elif row['aliquot_id'] == father_aliquot_id:
                    paternal_affected = row.get('affected_status', False)
            
            # Build analysis data for payload
            analysis_data = {
                'sequencing_id': proband_row['sequencing_id'],
                'analysis_id': analysis_id,
                'gender': proband_row.get('gender', 'unknown'),
                'clinical_signs': proband_row.get('clinical_signs', []),
                'maternal_affected': maternal_affected,
                'paternal_affected': paternal_affected
            }
            
            # Get VCF files for this analysis
            vcf_files = analysis_vcf_files.get(analysis_id, [])
            if not vcf_files:
                logging.warning(f'No VCF files found for analysis {analysis_id}, skipping')
                continue
            
            # Build and submit payload
            try:
                payload = build_phenovar_payload(analysis_data, vcf_files)
                response = submit_phenovar_analysis(payload)
                
                task_id = response.get('task_id')
                if task_id:
                    logging.info(f'Submitted analysis {analysis_id}, task_id: {task_id}')
                    write_s3_analysis_status(clin_s3, analysis_id, PhenotypingStatus.PENDING, task_id)
                    submitted_ids.append(analysis_id)
                else:
                    logging.error(f'No task_id in response for analysis {analysis_id}')
            
            except Exception as e:
                logging.error(f'Error submitting analysis {analysis_id}: {str(e)}')
        
        logging.info(f'Successfully submitted {len(submitted_ids)} analyses to Phenovar')
        return submitted_ids
    
    # Chain the tasks
    clinical_data = extract_clinical_data(analysis_ids, skip)
    vcf_data = copy_vcfs(clinical_data, skip)
    submit_analyses(vcf_data, skip)
