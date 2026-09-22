# -*- coding: utf-8 -*-
"""scripts/update_doc_relation.py must refuse bad inputs and prove its own writes.

Two Codex findings were filed against this step, and an independent audit refuted both
"fixes" at HEAD d8079cfe with reproductions:

* It read ``transcriptions_linked.csv`` with NO provenance check, so the stale or hand-edited
  derived file that ``import_pgp_full.py`` had just refused was accepted here and pushed to
  Supabase at exit 0 -- the user-visible misclassification this refresh exists to fix.
* "Fail when relation updates fail" held only for the exception path. A run that classified
  NOTHING (header-only CSV, renamed column) exited 0 with zero update calls, and success was
  inferred from ``response.data`` rather than from what the database held afterwards.

These tests run ``main()`` on a throwaway tree with a fake client, so a deleted call site
fails here rather than in production.
"""
from __future__ import annotations

import importlib.util
import io
import pathlib
import sys
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace

import pytest

import pgp_pipeline_fixtures as fx

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "update_doc_relation.py"


@pytest.fixture
def updater(monkeypatch):
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "not-a-real-key")
    spec = importlib.util.spec_from_file_location("_update_doc_relation", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeDocuments:
    """Just enough PostgREST for update(...).eq(...) and select(...).in_(...).

    `honest=False` models the failure the read-back exists to catch: the update call
    returns a row (so the caller counts a success) but the database does not change.
    """

    def __init__(self, rows, honest=True):
        self.rows = {row["pgpid"]: dict(row) for row in rows}
        self.honest = honest
        self.updates = 0
        self._result = None

    def table(self, _name):
        return self

    def update(self, payload):
        self._payload = payload
        return self

    def eq(self, _column, pgpid):
        row = self.rows.get(pgpid)
        self.updates += 1
        self._result = [dict(row, **self._payload)] if row is not None else []
        if row is not None and self.honest:
            row.update(self._payload)
        return self

    def select(self, _columns):
        return self

    def in_(self, _column, pgpids):
        self._result = [
            {"pgpid": p, "doc_relation": self.rows[p].get("doc_relation")}
            for p in pgpids if p in self.rows
        ]
        return self

    def execute(self):
        return SimpleNamespace(data=self._result)


def _point_at(updater, monkeypatch, root, client=None):
    pgp_data = root / "pgp_data"
    monkeypatch.setattr(updater, "PGP_DATA_DIR", pgp_data)
    monkeypatch.setattr(updater, "TRANSCRIPTIONS_CSV", pgp_data / "transcriptions_linked.csv")
    if client is None:
        def _never():
            raise AssertionError("the Supabase client must not be created on this path")
        monkeypatch.setattr(updater, "get_supabase_client", _never)
    else:
        monkeypatch.setattr(updater, "get_supabase_client", lambda: client)


def _run(updater, monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["update_doc_relation.py", *argv])
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        try:
            rc = updater.main()
        except SystemExit as exc:
            rc = exc.code
    return rc, out.getvalue(), err.getvalue()


# ── loading ───────────────────────────────────────────────────────────────────

def test_load_counts_every_row_it_cannot_use(updater, tmp_path):
    path = tmp_path / "linked.csv"
    path.write_text(
        "pgpid,doc_relation\n1,Digital Edition\n,Digital Edition\nabc,Digital Edition\n2,\n",
        encoding="utf-8",
    )
    relations, skipped = updater.load_doc_relations(path)
    assert relations == {1: "Digital Edition"}
    assert skipped == {"blank_pgpid": 1, "non_integer_pgpid": 1, "blank_doc_relation": 1}


def test_an_edition_is_never_demoted_by_a_later_translation(updater, tmp_path):
    path = tmp_path / "linked.csv"
    path.write_text(
        "pgpid,doc_relation\n1,Digital Edition\n1,Digital Translation\n"
        "2,Digital Translation\n2,Digital Edition\n",
        encoding="utf-8",
    )
    relations, _ = updater.load_doc_relations(path)
    assert relations == {1: "Digital Edition", 2: "Digital Edition"}


# ── the provenance check is WIRED IN ──────────────────────────────────────────

def test_a_derived_file_that_fails_its_provenance_is_refused_before_any_client_exists(
        updater, tmp_path, monkeypatch):
    """The exact route the audit ran: import_pgp_full.py refused this file; this script
    took it at exit 0 and pushed its classifications."""
    fx.build_tree(tmp_path)
    fx.tamper(tmp_path / "pgp_data" / "transcriptions_linked.csv")
    _point_at(updater, monkeypatch, tmp_path)  # client creation is an AssertionError

    rc, _out, err = _run(updater, monkeypatch, "--execute")
    assert rc == 1
    assert "do not match their provenance" in err
    assert "transcriptions_linked.csv" in err


def test_a_changed_libraries_csv_also_stops_the_classification(updater, tmp_path, monkeypatch):
    """Not an upstream file, so the upstream manifest cannot see it -- but it decides
    which manuscript every transcription is attributed to."""
    fx.build_tree(tmp_path)
    fx.tamper(tmp_path / "libraries.csv")
    _point_at(updater, monkeypatch, tmp_path)

    rc, _out, err = _run(updater, monkeypatch, "--execute")
    assert rc == 1
    assert "libraries.csv has changed" in err


def test_the_override_is_explicit_and_says_so(updater, tmp_path, monkeypatch):
    fx.build_tree(tmp_path)
    fx.tamper(tmp_path / "pgp_data" / "transcriptions_linked.csv")
    _point_at(updater, monkeypatch, tmp_path)

    rc, out, _err = _run(updater, monkeypatch, "--dry-run", "--no-provenance-check")
    assert rc == 0
    assert "WARNING: classifying from inputs that do not match" in out


# ── classifying nothing is not success ────────────────────────────────────────

def test_classifying_nothing_is_a_failure(updater, tmp_path, monkeypatch):
    """A header-only file (an upstream column rename produced one) used to exit 0 with
    zero update calls, and the next export shipped whatever doc_relation Supabase held."""
    fx.build_tree(tmp_path)
    fx.write_linked(tmp_path / "pgp_data" / "transcriptions_linked.csv", rows=[])
    fx.write_derived_stamp(tmp_path)  # so provenance passes and only the floor can fail
    _point_at(updater, monkeypatch, tmp_path)

    rc, _out, err = _run(updater, monkeypatch, "--dry-run")
    assert rc == 1
    assert "classifies nothing" in err


def test_an_unusable_row_is_a_failure_not_a_skip(updater, tmp_path, monkeypatch):
    fx.build_tree(tmp_path)
    rows = list(fx.LINKED_ROWS) + [dict(fx.LINKED_ROWS[0], pgpid="")]
    fx.write_linked(tmp_path / "pgp_data" / "transcriptions_linked.csv", rows=rows)
    fx.write_derived_stamp(tmp_path)
    _point_at(updater, monkeypatch, tmp_path)

    rc, _out, err = _run(updater, monkeypatch, "--dry-run")
    assert rc == 1
    assert "blank_pgpid: 1" in err


# ── the writes are proven by read-back ────────────────────────────────────────

def test_a_run_whose_updates_are_silently_ignored_fails(updater, tmp_path, monkeypatch):
    """update() returns a row (the old success signal) but the database is unchanged."""
    fx.build_tree(tmp_path)
    client = _FakeDocuments([{"pgpid": 1001, "doc_relation": "Digital Translation"}],
                            honest=False)
    _point_at(updater, monkeypatch, tmp_path, client=client)

    rc, _out, err = _run(updater, monkeypatch, "--execute")
    assert client.updates == 1, "the update was attempted"
    assert rc == 1
    assert "do not hold the classification" in err
    assert "wanted 'Digital Edition'" in err


def test_a_run_that_really_wrote_is_verified_and_passes(updater, tmp_path, monkeypatch):
    fx.build_tree(tmp_path)
    client = _FakeDocuments([{"pgpid": 1001, "doc_relation": None}])
    _point_at(updater, monkeypatch, tmp_path, client=client)

    rc, out, _err = _run(updater, monkeypatch, "--execute")
    assert rc == 0
    assert client.rows[1001]["doc_relation"] == "Digital Edition", (
        "the edition row wins over the translation row for the same pgpid"
    )
    assert "verified by read-back" in out


def test_an_unmatched_document_is_still_fatal(updater, tmp_path, monkeypatch):
    """Kept from round 3: load_doc_relations() collapses to one mapping per pgpid, so a
    no-match is a classification that was not applied, never a multi-fragment echo."""
    fx.build_tree(tmp_path)
    client = _FakeDocuments([])  # the document was never imported
    _point_at(updater, monkeypatch, tmp_path, client=client)

    rc, _out, err = _run(updater, monkeypatch, "--execute")
    assert rc == 1
    assert "matched no row" in err
