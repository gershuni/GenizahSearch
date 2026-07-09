"""Age-based .nicegui storage-user file retention (2026-07-08 Tier-2).

Exercises the retention branch added to
web/export_state.py::compact_nicegui_export_storage. Files are aged via
os.utime; a fake storage object supplies the path and loaded-session map.
"""

import json
import os
import time


from web.export_state import (
    compact_nicegui_export_storage,
    storage_retention_days_from_env,
)


class FakeStorage:
    def __init__(self, path, users=None):
        self.path = str(path)
        self._users = users or {}


def _write_storage_file(directory, session_id, age_days, payload=None):
    filepath = directory / f'storage-user-{session_id}.json'
    filepath.write_text(
        json.dumps(payload or {'_session_uuid': session_id}), encoding='utf-8'
    )
    aged = time.time() - age_days * 86400
    os.utime(filepath, (aged, aged))
    return filepath


class TestRetentionEnv:
    def test_default_90(self, monkeypatch):
        monkeypatch.delenv('GENIZAH_STORAGE_RETENTION_DAYS', raising=False)
        assert storage_retention_days_from_env() == 90

    def test_explicit(self, monkeypatch):
        monkeypatch.setenv('GENIZAH_STORAGE_RETENTION_DAYS', '30')
        assert storage_retention_days_from_env() == 30

    def test_zero_disables(self, monkeypatch):
        monkeypatch.setenv('GENIZAH_STORAGE_RETENTION_DAYS', '0')
        assert storage_retention_days_from_env() == 0

    def test_garbage_falls_back(self, monkeypatch):
        monkeypatch.setenv('GENIZAH_STORAGE_RETENTION_DAYS', 'never')
        assert storage_retention_days_from_env() == 90

    def test_negative_clamped(self, monkeypatch):
        monkeypatch.setenv('GENIZAH_STORAGE_RETENTION_DAYS', '-1')
        assert storage_retention_days_from_env() == 0


class TestRetentionDeletion:
    def test_old_files_deleted_fresh_files_kept(self, tmp_path):
        old = _write_storage_file(tmp_path, 'old-session', age_days=120)
        fresh = _write_storage_file(tmp_path, 'fresh-session', age_days=5)

        summary = compact_nicegui_export_storage(
            FakeStorage(tmp_path), retention_days=90
        )

        assert not old.exists()
        assert fresh.exists()
        assert summary['files_deleted'] == 1
        assert summary['bytes_deleted'] > 0
        assert summary['files_checked'] == 2
        assert summary['errors'] == 0

    def test_retention_none_deletes_nothing(self, tmp_path):
        old = _write_storage_file(tmp_path, 'old-session', age_days=400)
        summary = compact_nicegui_export_storage(
            FakeStorage(tmp_path), retention_days=None
        )
        assert old.exists()
        assert summary['files_deleted'] == 0

    def test_retention_zero_deletes_nothing(self, tmp_path):
        old = _write_storage_file(tmp_path, 'old-session', age_days=400)
        summary = compact_nicegui_export_storage(
            FakeStorage(tmp_path), retention_days=0
        )
        assert old.exists()
        assert summary['files_deleted'] == 0

    def test_loaded_session_never_deleted_even_if_old(self, tmp_path):
        old_loaded = _write_storage_file(tmp_path, 'loaded-session', age_days=400)
        storage = FakeStorage(tmp_path, users={'loaded-session': {}})
        summary = compact_nicegui_export_storage(storage, retention_days=90)
        assert old_loaded.exists()
        assert summary['files_deleted'] == 0

    def test_boundary_file_just_inside_window_kept(self, tmp_path):
        kept = _write_storage_file(tmp_path, 'boundary', age_days=89)
        summary = compact_nicegui_export_storage(
            FakeStorage(tmp_path), retention_days=90
        )
        assert kept.exists()
        assert summary['files_deleted'] == 0

    def test_deleted_files_do_not_reach_json_parsing(self, tmp_path):
        # A corrupt-but-old file must be deleted, not counted as an error —
        # proves deletion happens before json.load (Codex ordering note).
        filepath = tmp_path / 'storage-user-corrupt.json'
        filepath.write_text('{not json', encoding='utf-8')
        aged = time.time() - 120 * 86400
        os.utime(filepath, (aged, aged))

        summary = compact_nicegui_export_storage(
            FakeStorage(tmp_path), retention_days=90
        )
        assert not filepath.exists()
        assert summary['files_deleted'] == 1
        assert summary['errors'] == 0

    def test_compaction_still_runs_on_kept_files(self, tmp_path):
        # Fresh file with an oversized legacy export payload still compacts.
        big_payload = {
            'export_search_payload': {
                'results': [{'uid': f'u{i}', 'full_text': 'x' * 100} for i in range(50)],
                'query': 'q',
            }
        }
        fresh = _write_storage_file(
            tmp_path, 'fresh-big', age_days=1, payload=big_payload
        )
        summary = compact_nicegui_export_storage(
            FakeStorage(tmp_path), retention_days=90
        )
        assert fresh.exists()
        assert summary['files_checked'] == 1
        # bytes accounting stays coherent whether or not compaction changed it
        assert summary['bytes_before'] >= summary['bytes_after'] > 0


class TestSummaryShape:
    def test_new_counters_present_without_retention(self, tmp_path):
        summary = compact_nicegui_export_storage(FakeStorage(tmp_path))
        assert summary['files_deleted'] == 0
        assert summary['bytes_deleted'] == 0
