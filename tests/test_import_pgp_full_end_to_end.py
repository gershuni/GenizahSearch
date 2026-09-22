# -*- coding: utf-8 -*-
"""Drive the REAL ``import_pgp_full.py`` ``main()`` against a throwaway tree and a fake client.

Why: the import-provenance record is what lets the sidecar exporter stamp an upstream
commit into ``pgp.db``. An independent audit of HEAD d8079cfe showed the record was written
LAST, after all four passes, with nothing guarded in between -- so an override run that
died after the push (Ctrl-C, a PostgREST error, a locked ``full_import_report.txt``) left the
PREVIOUS record on disk with still-matching counts (upserts change content, not counts),
and the exporter corroborated it and stamped the rejected commit onto half-applied data.

Nothing tested ``main()``; the helpers were fine. These tests run it.
"""
from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import pathlib
import sys
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace

import pytest

import pgp_pipeline_fixtures as fx

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "import_pgp_full.py"
FAKE_URL = "http://127.0.0.1:9"  # nothing listens; the client is patched out anyway


class _FakeTable:
    def __init__(self, client, name):
        self.client = client
        self.name = name
        self._count_query = False

    def upsert(self, batch, on_conflict=None):
        if self.client.fail_on == self.name:
            raise RuntimeError("simulated PostgREST failure on %s" % self.name)
        store = self.client.stores.setdefault(self.name, {})
        keys = (on_conflict or "id").split(",")
        for record in batch:
            store[tuple(record.get(k) for k in keys)] = record
        self.client.upserts += len(batch)
        return self

    def select(self, *_a, **kwargs):
        self._count_query = kwargs.get("count") == "exact"
        return self

    def execute(self):
        if self._count_query:
            if self.client.counts_fail:
                return SimpleNamespace(count=None, data=[])
            return SimpleNamespace(count=len(self.client.stores.get(self.name, {})), data=[])
        return SimpleNamespace(count=None, data=[])


class _FakeSupabase:
    def __init__(self):
        self.stores = {}
        self.upserts = 0
        self.fail_on = None
        self.counts_fail = False

    def table(self, name):
        return _FakeTable(self, name)


@pytest.fixture
def importer(monkeypatch, tmp_path):
    """The real module, pointed at a throwaway project root, with Supabase patched out."""
    monkeypatch.setenv("SUPABASE_URL", FAKE_URL)
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "not-a-real-key")
    spec = importlib.util.spec_from_file_location("_import_pgp_full_e2e", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fx.build_tree(tmp_path)
    module.__file__ = str(tmp_path / "scripts" / "import_pgp_full.py")
    client = _FakeSupabase()
    module.create_client = lambda _url, _key: client
    module.fake_client = client
    return module


def _run(importer, monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["import_pgp_full.py", *argv])
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        rc = importer.main()
    return rc, out.getvalue(), err.getvalue()


def _record(tmp_path):
    path = tmp_path / "pgp_data" / "import_provenance.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def test_a_dry_run_pushes_nothing_and_records_nothing(importer, tmp_path, monkeypatch):
    rc, out, _ = _run(importer, monkeypatch)
    assert rc == 0
    assert "DRY RUN COMPLETE" in out
    assert importer.fake_client.upserts == 0
    assert _record(tmp_path) is None


def test_a_dry_run_writes_its_own_report_and_leaves_the_execute_report_alone(
        importer, tmp_path, monkeypatch):
    """The procedure says 'read the report, then --execute'. The dry run wrote none, so the
    file it pointed at was the PREVIOUS execute's report (Codex review 7, astra round 5)."""
    execute_report = tmp_path / "pgp_data" / "full_import_report.txt"
    execute_report.write_text("STALE REPORT FROM LAST WEEK", encoding="utf-8")

    assert _run(importer, monkeypatch)[0] == 0
    dry = (tmp_path / "pgp_data" / importer.DRY_RUN_REPORT_FILENAME).read_text(encoding="utf-8")
    assert "DRY RUN" in dry
    assert "Pass 1 - Documents:                 2" in dry
    assert execute_report.read_text(encoding="utf-8") == "STALE REPORT FROM LAST WEEK"


def test_a_completed_import_records_commit_verification_and_project(importer, tmp_path,
                                                                     monkeypatch):
    rc, _out, _ = _run(importer, monkeypatch, "--execute")
    assert rc == 0
    record = _record(tmp_path)
    assert record["upstream_commit"] == fx.COMMIT
    assert record["inputs_verified"] is True, "explicit, never implied by a missing key"
    assert record["supabase_url"] == FAKE_URL
    assert record["supabase_counts_after"]["documents"] == 2
    assert importer.fake_client.stores["documents"][(1001,)]["transcription"] == fx.EDITION_TEXT
    assert importer.fake_client.stores["documents"][(1001,)]["transcription_source"] == \
        "S. D. Goitein"


def test_an_import_interrupted_after_the_push_leaves_no_record_to_corroborate(
        importer, tmp_path, monkeypatch):
    """The refuted case. Run 1 completes and records commit A. Run 2 pushes Pass 1 and 2,
    then dies. The record from run 1 must be GONE -- with it on disk, the exporter saw
    matching counts and stamped commit A onto a database that no longer holds it."""
    assert _run(importer, monkeypatch, "--execute")[0] == 0
    assert _record(tmp_path)["upstream_commit"] == fx.COMMIT

    importer.fake_client.fail_on = "document_footnotes"  # Pass 3 of 4
    with pytest.raises(RuntimeError, match="simulated PostgREST failure"):
        _run(importer, monkeypatch, "--execute")

    assert _record(tmp_path) is None, (
        "an interrupted import must leave nothing for export_pgp_sidecar.py to corroborate"
    )


def test_a_locked_report_file_cannot_preserve_the_old_record(importer, tmp_path, monkeypatch):
    """Same failure, different trigger: full_import_report.txt is a tracked file operators
    open, and writing it is the step right before the record."""
    assert _run(importer, monkeypatch, "--execute")[0] == 0
    report = tmp_path / "pgp_data" / "full_import_report.txt"
    report.unlink()
    report.mkdir()  # the open() for writing now raises

    with pytest.raises(OSError):
        _run(importer, monkeypatch, "--execute")
    assert _record(tmp_path) is None


def test_an_override_import_that_completes_records_no_commit(importer, tmp_path, monkeypatch):
    fx.tamper(tmp_path / "pgp_data" / "documents.csv")
    rc, out, _ = _run(importer, monkeypatch, "--execute", "--no-provenance-check")
    assert rc == 0
    assert "WARNING: importing CSVs that do not match" in out
    record = _record(tmp_path)
    assert record["inputs_verified"] is False
    assert "upstream_commit" not in record


def test_mismatched_inputs_stop_the_import_before_the_first_push(importer, tmp_path,
                                                                 monkeypatch):
    fx.tamper(tmp_path / "pgp_data" / "documents.csv")
    rc, _out, err = _run(importer, monkeypatch, "--execute")
    assert rc == 1
    assert "SHA-256 does not match" in err
    assert importer.fake_client.upserts == 0
    assert _record(tmp_path) is None


def test_a_failed_count_query_is_not_recorded_as_zero(importer, tmp_path, monkeypatch):
    """capture_table_counts() swallowed every exception and wrote 0. Those numbers are the
    exporter's only corroboration, so a transient error turned a good import into one the
    exporter must reject -- or, before the push, into a report full of false deltas."""
    assert _run(importer, monkeypatch, "--execute")[0] == 0
    before = _record(tmp_path)

    importer.fake_client.counts_fail = True
    with pytest.raises(RuntimeError, match="no row count"):
        _run(importer, monkeypatch, "--execute")
    # The failure was at the BEFORE-counts, i.e. before anything was pushed or invalidated:
    # the previous record still honestly describes the database.
    assert _record(tmp_path) == before


def test_a_derived_file_with_an_unusable_row_is_refused(importer, tmp_path, monkeypatch):
    rows = list(fx.LINKED_ROWS) + [dict(fx.LINKED_ROWS[0], pgpid="not-a-number")]
    fx.write_linked(tmp_path / "pgp_data" / "transcriptions_linked.csv", rows=rows)
    fx.write_derived_stamp(tmp_path)
    with pytest.raises(SystemExit, match="no usable pgpid"):
        _run(importer, monkeypatch)


def test_the_importer_parses_exactly_the_bytes_it_verified(importer, tmp_path, monkeypatch):
    """Codex review 9: verify-then-reopen in import_pgp_full.py had the same
    swap-and-restore window. Every path-based loader is rigged to fail if main() touches
    it, and the bytes handed to one parser are compared to the manifest."""
    manifest = json.loads(
        (tmp_path / "pgp_data" / "upstream_provenance.json").read_text(encoding="utf-8")
    )
    for name in ("load_documents_full", "load_fragment_metadata", "load_footnotes",
                 "load_transcriptions", "load_genizahsearch_shelfmarks"):
        def by_path(*_a, _name=name, **_k):
            raise AssertionError("%s re-opened a file main() had already verified" % _name)
        monkeypatch.setattr(importer, name, by_path)

    seen = {}
    real = importer.load_documents_full_from_bytes

    def capture(raw):
        seen["documents.csv"] = raw
        return real(raw)

    monkeypatch.setattr(importer, "load_documents_full_from_bytes", capture)

    assert _run(importer, monkeypatch, "--execute")[0] == 0
    assert hashlib.sha256(seen["documents.csv"]).hexdigest() == \
        manifest["files"]["documents.csv"]["sha256"]
