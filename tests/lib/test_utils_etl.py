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


def test_chunk_list():
    """Test chunk_list function with various scenarios"""
    from dags.lib.utils_etl import chunk_list

    # Normal chunking with remainder
    assert chunk_list.function([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    
    # Exact multiple
    assert chunk_list.function([1, 2, 3, 4], 2) == [[1, 2], [3, 4]]
    
    # Empty list
    assert chunk_list.function([], 2) == []
    
    # List smaller than chunk size
    assert chunk_list.function([1, 2], 5) == [[1, 2]]
    
    # With strings (like analysis_ids)
    assert chunk_list.function(['id1', 'id2', 'id3', 'id4', 'id5', 'id6', 'id7'], 3) == [['id1', 'id2', 'id3'], ['id4', 'id5', 'id6'], ['id7']]
    
    # Single item
    assert chunk_list.function(['single_item'], 10) == [['single_item']]
    
    # Chunk size of 1
    assert chunk_list.function([1, 2, 3], 1) == [[1], [2], [3]]
    
    # Real-world Franklin scenario: 74 analysis_ids with chunk_size=15
    analysis_ids = [f'analysis_{i}' for i in range(1, 75)]
    result = chunk_list.function(analysis_ids, 15)
    assert len(result) == 5
    assert [len(chunk) for chunk in result] == [15, 15, 15, 15, 14]
    assert sum(len(chunk) for chunk in result) == 74