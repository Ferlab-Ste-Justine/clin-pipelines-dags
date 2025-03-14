from lib.config_nextflow import nextflow_variant_annotation_revision, nextflow_variant_annotation_pipeline, \
    nextflow_variant_annotation_config_file, nextflow_variant_annotation_config_map, \
    nextflow_variant_annotation_params_file
from lib.config_operators import nextflow_base_config
from lib.operators.nextflow import NextflowOperator


def run(input: str, outdir: str, skip: str = '', **kwargs):
    nextflow_base_config \
        .with_pipeline(nextflow_variant_annotation_pipeline) \
        .with_revision(nextflow_variant_annotation_revision) \
        .append_config_maps(nextflow_variant_annotation_config_map) \
        .append_config_files(nextflow_variant_annotation_config_file) \
        .with_params_file(nextflow_variant_annotation_params_file) \
        .append_args(
            '--input', input,
            '--outdir', outdir
        ) \
        .operator(
            NextflowOperator,
            task_id='variant_annotation',
            name='variant_annotation',
            skip=skip,
            **kwargs
        )