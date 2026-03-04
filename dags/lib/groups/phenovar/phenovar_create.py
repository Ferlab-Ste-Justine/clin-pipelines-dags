import logging
from typing import List

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException, AirflowSkipException
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
        Filters out analyses that already have result files.
        """
        # Local imports for virtualenv
        import logging
        from airflow.exceptions import AirflowSkipException
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from lib import config
        from lib.config import clin_datalake_bucket
        from lib.phenovar import get_clinical_data, build_s3_result_key
        
        if _skip:
            raise AirflowSkipException()
        
        # Filter out analyses that already have results
        clin_s3 = S3Hook(config.s3_conn_id)
        analyses_to_process = []
        for analysis_id in _analysis_ids:
            result_key = build_s3_result_key(analysis_id)
            if clin_s3.check_for_key(result_key, clin_datalake_bucket):
                logging.info(f'Analysis {analysis_id} already has result file, skipping')
            else:
                analyses_to_process.append(analysis_id)
        
        if not analyses_to_process:
            logging.info('All analyses already have results, returning empty list')
            return {}
        
        logging.info(f'Processing {len(analyses_to_process)} analyses (skipped {len(_analysis_ids) - len(analyses_to_process)} with existing results)')
        
        clinical_data = get_clinical_data(analyses_to_process)
        logging.info(f'Extracted clinical data for {len(clinical_data)} records')
        
        # Group by analysis_id to handle trios (multiple rows per analysis)
        analysis_groups = {}
        for row in clinical_data:
            analysis_id = row['analysis_id']
            if analysis_id not in analysis_groups:
                analysis_groups[analysis_id] = []
            analysis_groups[analysis_id].append(row)
        
        logging.info(f'Grouped into {len(analysis_groups)} analyses')
        
        # Filter out families with siblings (keep only SOLO/DUO/TRIO)
        filtered_groups = {}
        for analysis_id, rows in analysis_groups.items():
            has_proband = False
            has_mother = False
            has_father = False
            has_sibling = False
            
            # Find proband to get parent info
            proband_row = next((r for r in rows if r['is_proband']), None)
            if proband_row:
                has_proband = True
                if proband_row.get('father_aliquot_id'):
                    has_father = True
                if proband_row.get('mother_aliquot_id'):
                    has_mother = True
            
            # Check for siblings (non-proband with parent info)
            for row in rows:
                if not row['is_proband'] and (row.get('father_aliquot_id') or row.get('mother_aliquot_id')):
                    has_sibling = True
                    logging.warning(f'Sibling detected in analysis {analysis_id} for sequencing_id {row["sequencing_id"]}, skipping family')
                    break
            
            # Validate HPO codes (required by Phenovar API)
            has_hpo = proband_row and proband_row.get('clinical_signs')
            if not has_hpo:
                logging.warning(f'No HPO codes for analysis {analysis_id}, skipping (required by Phenovar)')
                continue
            
            # Validate VCF files exist
            has_vcf = False
            for row in rows:
                snv_urls = row.get('snv_vcf_germline_urls')
                cnv_urls = row.get('cnv_vcf_germline_urls')
                if (snv_urls and len(snv_urls) > 0) or (cnv_urls and len(cnv_urls) > 0):
                    has_vcf = True
                    break
            
            if not has_vcf:
                logging.warning(f'No VCF files for analysis {analysis_id}, skipping')
                continue
            
            # Only keep SOLO, DUO, or TRIO (no siblings)
            if has_proband and not has_sibling:
                filtered_groups[analysis_id] = rows
            else:
                logging.warning(
                    f'Skipping analysis {analysis_id}: PROBAND={has_proband} MOTHER={has_mother} '
                    f'FATHER={has_father} SIBLING={has_sibling}')
        
        logging.info(f'Filtered to {len(filtered_groups)} valid analyses (SOLO/DUO/TRIO with HPO codes and VCF files)')
        return filtered_groups
    
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
            
            errors = []
            
            for row in rows:
                aliquot_id = row['aliquot_id']
                is_proband = row['is_proband']
                is_mother = aliquot_id == mother_aliquot_id
                is_father = aliquot_id == father_aliquot_id
                
                # Process SNV VCF
                snv_urls = row.get('snv_vcf_germline_urls')
                if snv_urls:
                    # Extract first URL from list (converted from numpy array)
                    snv_url_str = snv_urls[0] if isinstance(snv_urls, list) and len(snv_urls) > 0 else str(snv_urls)
                    if snv_url_str and snv_url_str != 'None':
                        try:
                            source_bucket, source_key = parse_s3_url(snv_url_str)
                            filename = extract_vcf_filename(source_key)
                            
                            # Copy and potentially gzip, get actual filename used
                            actual_filename = copy_vcf_to_phenovar_bucket(
                                clin_s3, phenovar_s3, analysis_id,
                                source_bucket, source_key, filename
                            )
                            
                            vcf_files.append({
                                'filepath': f'{analysis_id}/{actual_filename}',
                                'filetype': map_phenovar_file_type(is_proband, is_mother, is_father, is_snv=True)
                            })
                        except Exception as e:
                            error_msg = f'Error processing SNV VCF for {aliquot_id}: {str(e)}'
                            logging.error(error_msg)
                            errors.append(error_msg)
                
                # Process CNV VCF
                cnv_urls = row.get('cnv_vcf_germline_urls')
                if cnv_urls:
                    # Extract first URL from list (converted from numpy array)
                    cnv_url_str = cnv_urls[0] if isinstance(cnv_urls, list) and len(cnv_urls) > 0 else str(cnv_urls)
                    if cnv_url_str and cnv_url_str != 'None':
                        try:
                            source_bucket, source_key = parse_s3_url(cnv_url_str)
                            filename = extract_vcf_filename(source_key)
                            
                            # Copy and potentially gzip, get actual filename used
                            actual_filename = copy_vcf_to_phenovar_bucket(
                                clin_s3, phenovar_s3, analysis_id,
                                source_bucket, source_key, filename
                            )
                            
                            vcf_files.append({
                                'filepath': f'{analysis_id}/{actual_filename}',
                                'filetype': map_phenovar_file_type(is_proband, is_mother, is_father, is_snv=False)
                            })
                        except Exception as e:
                            error_msg = f'Error processing CNV VCF for {aliquot_id}: {str(e)}'
                            logging.error(error_msg)
                            errors.append(error_msg)
            
            # Fail if there were any errors during VCF processing
            if errors:
                raise AirflowFailException(f'Failed to copy VCFs for analysis {analysis_id}: {"; ".join(errors)}')
            
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
                'age_at_onset_codes': proband_row.get('age_at_onset_codes', []),
                'maternal_affected': maternal_affected,
                'paternal_affected': paternal_affected
            }
            
            # Get VCF files for this analysis
            vcf_files = analysis_vcf_files.get(analysis_id, [])
            if not vcf_files:
                logging.warning(f'No VCF files found for analysis {analysis_id}, skipping')
                continue
            
            # Build and submit payload
            payload = build_phenovar_payload(analysis_data, vcf_files)
            response = submit_phenovar_analysis(payload)
            
            task_id = response.get('task_id')
            if task_id:
                logging.info(f'Submitted analysis {analysis_id}, task_id: {task_id}')
                write_s3_analysis_status(clin_s3, analysis_id, PhenotypingStatus.PENDING, task_id)
                submitted_ids.append(analysis_id)
            else:
                raise AirflowFailException(f'No task_id in response for analysis {analysis_id}')
        
        logging.info(f'Successfully submitted {len(submitted_ids)} analyses to Phenovar')
        return submitted_ids
    
    # Chain the tasks
    clinical_data = extract_clinical_data(analysis_ids, skip)
    vcf_data = copy_vcfs(clinical_data, skip)
    submit_analyses(vcf_data, skip)
