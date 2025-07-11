from unittest.mock import MagicMock


def test_get_batch_ids():
    from dags.lib.tasks.params import get_batch_ids

    mock_ti = MagicMock()
    mock_ti.dag_run.conf = {
        'batch_ids': ['batch_1', 'batch_2_somatic_normal', 'batch_3']
    }
    batch_ids = get_batch_ids.function(mock_ti)

    assert batch_ids == ['batch_1', 'batch_3', 'batch_2_somatic_normal']  # somatic_normal should be last


def test_get_sequencing_ids():
    from dags.lib.tasks.params import get_sequencing_ids

    mock_ti = MagicMock()
    mock_ti.dag_run.conf = {
        'sequencing_ids': ['seq_1', 'seq_2']
    }
    sequencing_ids = get_sequencing_ids.function(mock_ti)

    assert sequencing_ids == ['seq_1', 'seq_2']  # should return the list as is
