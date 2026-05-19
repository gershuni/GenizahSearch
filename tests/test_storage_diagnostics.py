from pathlib import Path
from types import SimpleNamespace

from web.storage_diagnostics import summarize_nicegui_storage


class DummyStorage:
    def __init__(self, path: Path, users=None, tabs=None):
        self.path = path
        self._users = users or {}
        self._tabs = tabs or {}


class DummyPayload(dict):
    def __init__(self, *args, last_modified: float | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_modified = last_modified


def _client(session_id: str):
    return SimpleNamespace(request=SimpleNamespace(session={'id': session_id}))


def test_summarize_nicegui_storage_reports_counts_and_sizes(tmp_path):
    storage_path = tmp_path / '.nicegui'
    storage_path.mkdir()
    (storage_path / 'storage-user-active.json').write_text('{"a":1}', encoding='utf-8')
    (storage_path / 'storage-user-stale.json').write_text('{"b":2,"c":3}', encoding='utf-8')

    storage = DummyStorage(
        storage_path,
        users={
            'active': DummyPayload({'search_history': ['x']}, last_modified=1_000.0),
            'stale': DummyPayload({'search_history': ['x'] * 3}, last_modified=2_000.0),
        },
        tabs={'tab-1': {'snapshot': True}},
    )

    summary = summarize_nicegui_storage(storage, [_client('active')])

    assert summary['nicegui_client_count'] == 1
    assert summary['nicegui_user_storage_count'] == 2
    assert summary['nicegui_retained_user_storage_count'] == 1
    assert summary['nicegui_tab_storage_count'] == 1
    assert summary['nicegui_storage_file_count'] == 2
    assert summary['nicegui_user_storage_file_count'] == 2
    assert summary['nicegui_storage_total_bytes'] > 0
    assert summary['nicegui_user_storage_payload_total_bytes_estimate'] > 0
    assert summary['nicegui_top_user_sessions'][0]['session_id'] == 'stale'
    assert summary['nicegui_top_user_sessions'][0]['has_active_client'] is False
    assert summary['nicegui_top_user_sessions'][0]['top_keys'][0]['key'] == 'search_history'
    assert summary['nicegui_top_storage_files'][0]['name'] == 'storage-user-stale.json'


def test_summarize_nicegui_storage_handles_unserializable_payloads(tmp_path):
    storage = DummyStorage(
        tmp_path / '.nicegui',
        users={'broken': DummyPayload({'callback': lambda: None}, last_modified=3_000.0)},
    )

    summary = summarize_nicegui_storage(storage, [])

    assert summary['nicegui_user_storage_count'] == 1
    assert summary['nicegui_top_user_sessions'][0]['bytes_estimate'] is None
    assert summary['nicegui_retained_user_storage_count'] == 1
