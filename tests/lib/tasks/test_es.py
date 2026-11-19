from unittest import mock

import pytest
from airflow.exceptions import AirflowFailException, AirflowSkipException

ALLOCATION = """shards disk.indices disk.used disk.avail disk.total disk.percent host     ip       node
    15      424.7gb   427.5gb    105.3gb    532.8gb           {first} 10.0.0.1 10.0.0.1 worker-1
    14        428gb   431.8gb      101gb    532.8gb           {second} 10.0.0.2 10.0.0.2 worker-2
     3                                                                                   UNASSIGNED
"""


def _response(status_code=200, json=None, text=''):
    response = mock.Mock()
    response.status_code = status_code
    response.ok = status_code < 400
    response.json.return_value = json
    response.text = text
    return response


def test_get_previous_release():
    from dags.lib.tasks.es import get_previous_release
    assert get_previous_release('re_010') == 're_008'
    assert get_previous_release('re_010', 1) == 're_009'


def test_get_previous_release_invalid_format():
    from dags.lib.tasks.es import get_previous_release
    with pytest.raises(AirflowFailException):
        get_previous_release('010')


def test_get_previous_release_below_zero():
    from dags.lib.tasks.es import get_previous_release
    with pytest.raises(AirflowSkipException):
        get_previous_release('re_000', 1)


def test_delete_previous_release():
    from dags.lib.tasks.es import delete_previous_release
    with mock.patch('dags.lib.tasks.es.requests') as requests:
        requests.get.return_value = _response(json={'clin_test_variant_centric_re_010': {}})
        requests.delete.return_value = _response()
        delete_previous_release.function('variant_centric', 're_010', '')
        assert '/clin_test_variant_centric_re_008?' in requests.delete.call_args.args[0]


def test_delete_previous_release_without_alias():
    from dags.lib.tasks.es import delete_previous_release
    with mock.patch('dags.lib.tasks.es.requests') as requests:
        requests.get.return_value = _response(status_code=404)
        requests.delete.return_value = _response()
        delete_previous_release.function('variant_centric', 're_010', '_blue')
        assert '/clin_test_blue_variant_centric_re_008?' in requests.delete.call_args.args[0]


def test_delete_previous_release_refuses_live_index():
    from dags.lib.tasks.es import delete_previous_release
    with mock.patch('dags.lib.tasks.es.requests') as requests:
        requests.get.return_value = _response(json={'clin_test_variant_centric_re_008': {}})
        with pytest.raises(AirflowFailException):
            delete_previous_release.function('variant_centric', 're_010', '')
        requests.delete.assert_not_called()


def test_delete_previous_release_skip():
    from dags.lib.tasks.es import delete_previous_release
    with mock.patch('dags.lib.tasks.es.requests') as requests:
        with pytest.raises(AirflowSkipException):
            delete_previous_release.function('variant_centric', 're_010', '', skip='yes')
        requests.delete.assert_not_called()


def test_delete_release_refuses_all_when_one_is_live():
    from dags.lib.tasks.es import delete_release
    with mock.patch('dags.lib.tasks.es.requests') as requests:
        requests.get.side_effect = [_response(status_code=404),
                                    _response(json={'clin_test_variant_centric_re_010': {}})]
        with pytest.raises(AirflowFailException):
            delete_release.function(['gene_centric', 'variant_centric'], 're_010', '')
        requests.delete.assert_not_called()


def test_delete_release():
    from dags.lib.tasks.es import delete_release
    with mock.patch('dags.lib.tasks.es.requests') as requests:
        requests.get.return_value = _response(json={'clin_test_variant_centric_re_010': {}})
        requests.delete.return_value = _response()
        delete_release.function(['gene_centric', 'variant_centric'], 're_009', '')
        deleted = [call.args[0] for call in requests.delete.call_args_list]
        assert len(deleted) == 2
        assert '/clin_test_gene_centric_re_009?' in deleted[0]
        assert '/clin_test_variant_centric_re_009?' in deleted[1]


def test_delete_release_invalid_release():
    from dags.lib.tasks.es import delete_release
    with mock.patch('dags.lib.tasks.es.requests') as requests:
        with pytest.raises(AirflowFailException):
            delete_release.function(['variant_centric'], '', '')
        requests.delete.assert_not_called()


def test_set_previous_release_replicas():
    from dags.lib.tasks.es import set_previous_release_replicas
    with mock.patch('dags.lib.tasks.es.requests') as requests:
        requests.put.return_value = _response()
        set_previous_release_replicas.function('variant_centric', 're_010', '', 0)
        assert '/clin_test_variant_centric_re_009/_settings' in requests.put.call_args.args[0]
        assert requests.put.call_args.kwargs['json'] == {'index': {'number_of_replicas': 0}}


def test_set_previous_release_replicas_missing_index():
    from dags.lib.tasks.es import set_previous_release_replicas
    with mock.patch('dags.lib.tasks.es.requests') as requests:
        requests.put.return_value = _response(status_code=404)
        with pytest.raises(AirflowSkipException):
            set_previous_release_replicas.function('variant_centric', 're_010', '', 0)


def test_disk_usage_uses_max_across_nodes():
    from dags.lib.tasks.es import test_disk_usage as disk_usage
    with mock.patch('dags.lib.tasks.es.requests') as requests:
        requests.get.return_value = _response(text=ALLOCATION.format(first=80, second=82))
        with pytest.raises(AirflowFailException):
            disk_usage.function()


def test_disk_usage_ok():
    from dags.lib.tasks.es import test_disk_usage as disk_usage
    with mock.patch('dags.lib.tasks.es.requests') as requests:
        requests.get.return_value = _response(text=ALLOCATION.format(first=80, second=61))
        disk_usage.function()
