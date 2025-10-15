from unittest import mock


def test_get_batch_ids():
    from dags.lib.tasks.params import get_batch_ids
    mock_context = {
        "params": {
            "batch_ids": ['batch_1', 'batch_2_somatic_normal', 'batch_3']
        }
    }

    with mock.patch("dags.lib.tasks.params.get_current_context", return_value=mock_context):
        batch_ids = get_batch_ids.function()
        assert batch_ids == ['batch_1', 'batch_3', 'batch_2_somatic_normal']  # somatic_normal should be last


def test_get_sequencing_ids():
    from dags.lib.tasks.params import get_sequencing_ids
    mock_context = {
        "params": {
            'sequencing_ids': ['seq_1', 'seq_2']
        }
    }

    with mock.patch("dags.lib.tasks.params.get_current_context", return_value=mock_context):
        sequencing_ids = get_sequencing_ids.function()
        assert sequencing_ids == ['seq_1', 'seq_2']  # should return the list as is

def test_get_analysis_ids():
    from dags.lib.tasks.params import get_analysis_ids
    mock_context = {
        "params": {
            'analysis_ids': ['ana_1', 'ana_2']
        }
    }

    with mock.patch("dags.lib.tasks.params.get_current_context", return_value=mock_context):
        analysis_ids = get_analysis_ids.function()
        assert analysis_ids == ['ana_1', 'ana_2']  # should return the list as is