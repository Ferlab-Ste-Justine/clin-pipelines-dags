from jinja2 import Template


def test_skip_import_true():
    from dags.lib.utils_etl import skip_import

    params = {
        'batch_id': 'batch_1',
        'import': 'no'
    }
    template = Template(skip_import())
    rendered = template.render(params=params)

    assert rendered == 'yes'


def test_skip_import_false():
    from dags.lib.utils_etl import skip_import

    params = {
        'batch_ids': ['batch_1', 'batch_2'],
        'import': 'yes'
    }
    template = Template(skip_import(batch_param_name='batch_ids'))
    rendered = template.render(params=params)

    assert rendered == ''


def test_skip_import_no_batch_id():
    from dags.lib.utils_etl import skip_import

    params = {
        'batch_id': '',
        'import': 'yes'
    }
    template = Template(skip_import())
    rendered = template.render(params=params)

    assert rendered == 'yes'


def test_skip_import_no_batch_ids():
    from dags.lib.utils_etl import skip_import

    params = {
        'batch_ids': [],
        'import': 'yes'
    }
    template = Template(skip_import(batch_param_name='batch_ids'))
    rendered = template.render(params=params)

    assert rendered == 'yes'


def test_batch_folder_exists_true(clin_minio, clean_up_clin_minio):
    from dags.lib.utils_etl import batch_folder_exists
    from lib.config import clin_import_bucket

    batch_id = "BATCH_1"
    vcf_path = f"{batch_id}/1.case.hard-filtered.formatted.norm.VEP.vcf.gz"
    clin_minio.load_string("{}", key=vcf_path, bucket_name=clin_import_bucket)

    assert batch_folder_exists(clin_minio, batch_id) is True


def test_batch_folder_exists_false(clin_minio, clean_up_clin_minio):
    from dags.lib.utils_etl import batch_folder_exists

    batch_id = "BATCH_1"
    assert batch_folder_exists(clin_minio, batch_id) is False