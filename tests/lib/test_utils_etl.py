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
