import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest import mock

MODULE = 'lib.tasks.public_data'


def test_every_published_source_is_registered():
    """A published source missing from the registry would be missing from public-databases.json, and
    its first run would then fall back to creating its own entry. Sources opting out with
    add_to_file=False (gnomAD constraint / CNV / SV, HPO terms) must stay out."""
    from airflow.models import DagBag
    from lib.tasks.public_data import PublicSourceDag, dags_folder

    DagBag(dag_folder=str(dags_folder), include_examples=False)

    published, opted_out = set(), set()
    for path in dags_folder.glob('etl_import_*.py'):
        declaration = path.read_text()
        if 'PublicSourceDag(' in declaration:
            (opted_out if 'add_to_file=False' in declaration else published).add(path.stem)

    registered = {source.dag_id for source in PublicSourceDag.registered()}

    assert published, 'no DAG file declares a published PublicSourceDag — the glob is wrong'
    assert published <= registered
    assert not opted_out & registered


def _source(name='cosmic', display_name='COSMIC', website='https://www.cosmickb.org/', **kwargs):
    from lib.tasks.public_data import PublicSourceDag
    return PublicSourceDag(name=name, display_name=display_name, website=website,
                           add_to_file=False, **kwargs)


def _entry(**kwargs):
    from lib.tasks.public_data import PublicSourceInfo
    defaults = {
        'dag_id': 'etl_import_cosmic',
        'source': 'COSMIC',
        'url': 'https://www.cosmickb.org/',
        'version': 'v104',
        'lastUpdate': '2026-07-30T15:12:03.123456',
        'frequency': None,
    }
    return PublicSourceInfo(**{**defaults, **kwargs})


@contextmanager
def _mocked_sync(sources, entries, current_version='v104'):
    """Isolate sync_public_databases_file from S3 and from the real DagBag."""
    from lib.tasks.public_data import PublicSourceDag
    for source in sources:
        source.get_current_version = mock.Mock(return_value=current_version)
    with mock.patch(f'{MODULE}.s3') as s3, \
            mock.patch(f'{MODULE}.DagBag') as dag_bag, \
            mock.patch(f'{MODULE}._get_public_data_json', return_value=entries), \
            mock.patch(f'{MODULE}._init_last_update', side_effect=lambda info: info) as init_last_update, \
            mock.patch.object(PublicSourceDag, 'registered', return_value=sources):
        dag_bag.return_value.import_errors = {}
        yield SimpleNamespace(s3=s3, init_last_update=init_last_update)


def _sync(sources, entries, current_version='v104'):
    from lib.tasks.public_data import sync_public_databases_file
    with _mocked_sync(sources, entries, current_version) as m:
        sync_public_databases_file.function()
    return m


def _written(s3) -> list[dict]:
    s3.load_string.assert_called_once()
    return json.loads(s3.load_string.call_args.args[0])


def test_sync_does_not_write_when_nothing_changed():
    """Runs daily: an unconditional rewrite would race with the version / lastUpdate published by
    update_public_data_info."""
    source = _source()

    m = _sync([source], [_entry()])

    m.s3.load_string.assert_not_called()
    source.get_current_version.assert_not_called()
    m.init_last_update.assert_not_called()


def test_sync_writes_once_for_a_legacy_empty_frequency():
    """update_public_data_info used to write "" where the sync writes None; the first run after the
    fix normalizes it, and the next one is a no-op."""
    source = _source()
    entries = [_entry(frequency='')]

    m = _sync([source], entries)
    assert _written(m.s3)[0]['frequency'] is None

    m = _sync([source], entries)
    m.s3.load_string.assert_not_called()


def test_sync_refreshes_metadata_without_touching_the_published_run():
    source = _source()

    m = _sync([source], [_entry(url='https://cancer.sanger.ac.uk/cosmic')])

    written = _written(m.s3)[0]
    assert written['url'] == 'https://www.cosmickb.org/'
    assert written['version'] == 'v104'
    assert written['lastUpdate'] == '2026-07-30T15:12:03.123456'


def test_sync_appends_a_missing_source():
    source = _source()

    m = _sync([source], [_entry(dag_id='etl_import_clinvar', source='NCBI Clinvar')],
              current_version='v105')

    written = _written(m.s3)
    assert [e['dag_id'] for e in written] == ['etl_import_clinvar', 'etl_import_cosmic']
    assert written[1]['version'] == 'v105'
    m.init_last_update.assert_called_once()


def test_sync_writes_the_whole_file_once():
    """The point of the DAG: one write for every source, not one read-modify-write per source."""
    sources = [_source(), _source(name='clinvar', display_name='NCBI Clinvar', website='https://ncbi')]

    m = _sync(sources, [_entry(url='stale'), _entry(dag_id='etl_import_clinvar', source='stale')])

    written = _written(m.s3)
    assert [e['dag_id'] for e in written] == ['etl_import_cosmic', 'etl_import_clinvar']
    assert [e['source'] for e in written] == ['COSMIC', 'NCBI Clinvar']


def test_update_public_data_info_publishes_only_the_run_outcome():
    """The task owns version / lastUpdate: writing frequency here is what used to make the two
    writers disagree on an empty schedule and rewrite the file forever."""
    from lib.tasks.public_data import update_public_data_info
    entries = [_entry(version='v103')]
    dag_data = _source(last_version='v104')
    context = {'dag': SimpleNamespace(dag_id='etl_import_cosmic', schedule_interval='0 6 * * 6')}

    with mock.patch(f'{MODULE}.s3') as s3, \
            mock.patch(f'{MODULE}._get_public_data_json', return_value=entries):
        update_public_data_info.function(dag_data, **context)

    written = _written(s3)[0]
    assert written['version'] == 'v104'
    assert written['lastUpdate'] != '2026-07-30T15:12:03.123456'
    assert written['frequency'] is None
    assert written['source'] == 'COSMIC'
    assert written['url'] == 'https://www.cosmickb.org/'


def test_update_public_data_info_creates_a_missing_entry():
    """A source added between two runs of the init DAG must not fail its first import."""
    from lib.tasks.public_data import update_public_data_info
    entries = [_entry(dag_id='etl_import_clinvar', source='NCBI Clinvar')]
    dag_data = _source(last_version='v104')
    dag_data.get_current_version = mock.Mock(return_value='v104')
    context = {'dag': SimpleNamespace(dag_id='etl_import_cosmic', schedule_interval='0 6 * * 6')}

    with mock.patch(f'{MODULE}.s3') as s3, \
            mock.patch(f'{MODULE}._get_public_data_json', return_value=entries):
        update_public_data_info.function(dag_data, **context)

    written = _written(s3)[1]
    assert written['dag_id'] == 'etl_import_cosmic'
    assert written['source'] == 'COSMIC'
    assert written['version'] == 'v104'
    assert written['lastUpdate']
    # serialize() drops 'schedule', so the frequency comes from the DAG's own interval
    assert written['frequency'] == '0 6 * * 6'
