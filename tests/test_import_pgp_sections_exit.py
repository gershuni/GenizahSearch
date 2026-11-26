"""import_pgp_sections.py --execute must exit non-zero when any update fails.

refresh_pgp_data.ps1 stops only on a non-zero exit. On 2026-11-26 one update raised
(a dropped connection), the script printed "1 errors during update", exited 0, and the
refresh went on to export a sidecar with that row's sections stale.
"""
import importlib.util
import os

import pytest

_PATH = os.path.join(os.path.dirname(__file__), "..", "scripts", "import_pgp_sections.py")


def _load():
    pytest.importorskip("supabase")
    spec = importlib.util.spec_from_file_location("import_pgp_sections_under_test", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Query:
    def __init__(self, client, rid):
        self.client, self.rid = client, rid

    def update(self, _payload):
        return self

    def eq(self, _col, rid):
        self.rid = rid
        return self

    def execute(self):
        self.client.calls.append(self.rid)
        if self.rid in self.client.fail_ids:
            raise ConnectionError("ConnectionTerminated")
        if self.rid in self.client.fail_once and self.client.calls.count(self.rid) == 1:
            raise ConnectionError("ConnectionTerminated")
        if self.rid in self.client.no_row_ids:
            return _Resp([])
        return _Resp([{"id": self.rid}])


class _Resp:
    def __init__(self, data):
        self.data = data


class _Client:
    def __init__(self, fail_ids, fail_once=(), no_row_ids=()):
        self.fail_ids = set(fail_ids)
        self.fail_once = set(fail_once)
        self.no_row_ids = set(no_row_ids)
        self.calls = []

    def table(self, _name):
        return _Query(self, None)


def _run_main(mod, monkeypatch, tmp_path, fail_ids, fail_once=(), no_row_ids=()):
    updates = [{"id": i, "pgpid": 100 + i, "sections": [], "source_language": None,
                "source_direction": None} for i in (1, 2, 3)]
    stats = {"pgpids_matched": 3, "pgpids_unmatched_in_db": 0, "editions_updated": 3,
             "translations_updated": 0}
    client = _Client(fail_ids, fail_once, no_row_ids)
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test-not-a-key")
    monkeypatch.setattr(mod, "clone_or_update_pgp_text", lambda _d: True)
    monkeypatch.setattr(mod, "scan_html_files",
                        lambda _d: {101: [{"type": "transcription"}]})
    monkeypatch.setattr(mod, "create_client", lambda _u, _k: client)
    monkeypatch.setattr(mod, "load_existing_sources", lambda _c: {101: [{}]})
    monkeypatch.setattr(mod, "build_update_batches", lambda _f, _s: (updates, stats))
    monkeypatch.setattr(mod, "write_verification_report", lambda *a, **k: None)
    return mod.main(dry_run=False), client


def test_a_failed_update_makes_execute_exit_non_zero(monkeypatch, tmp_path, capsys):
    mod = _load()
    code, client = _run_main(mod, monkeypatch, tmp_path, fail_ids={2})
    assert client.calls == [1, 2, 2, 3]
    assert code == 1
    assert "IMPORT INCOMPLETE: 1 of 3" in capsys.readouterr().out


def test_a_dropped_connection_is_retried_once(monkeypatch, tmp_path, capsys):
    """The server's GOAWAY after 10,000 requests: the retry reconnects and applies it."""
    mod = _load()
    code, client = _run_main(mod, monkeypatch, tmp_path, fail_ids=set(), fail_once={2})
    assert client.calls == [1, 2, 2, 3]
    assert code == 0
    assert "IMPORT COMPLETE" in capsys.readouterr().out


def test_an_update_that_matched_no_row_is_a_failure(monkeypatch, tmp_path, capsys):
    """PostgREST returns success with no rows for a missing or RLS-hidden row (Codex, #149)."""
    mod = _load()
    code, _ = _run_main(mod, monkeypatch, tmp_path, fail_ids=set(), no_row_ids={3})
    assert code == 1
    assert "IMPORT INCOMPLETE: 1 of 3" in capsys.readouterr().out


def test_all_updates_applied_exits_zero(monkeypatch, tmp_path, capsys):
    mod = _load()
    code, _ = _run_main(mod, monkeypatch, tmp_path, fail_ids=set())
    assert code == 0
    assert "IMPORT COMPLETE" in capsys.readouterr().out
