# -*- coding: utf-8 -*-
"""The owner's withholding decision must survive a build on any machine.

On 2026-09-21 the owner ruled that the regenerated Hebrew PGP translations do not ship:
~28% of them are materially wrong. The website never had them, and the desktop was handled
by lifting ``pgp_translations`` out of ``pgp_data/pgp.db``, because ``GenizahSearchPro.spec``
bundles that file into every installer.

But ``pgp_data/*.db`` is gitignored. The removal is therefore local file state, not a
property of the repository -- so nothing stopped a build host that kept the old sidecar,
or anyone who ran ``scripts/restore_pgp_translations.py`` to measure against the old
corpus, from shipping it anyway. A decision that a routine build can silently reverse is
not enforced.

These tests pin the enforcement, and that ``build_app.bat`` actually invokes it.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sqlite3

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_shipping_sidecar.py"
BUILD_BAT = REPO_ROOT / "build_app.bat"


@pytest.fixture(scope="module")
def guard():
    """Import by path -- scripts/ is a flat namespace package with no __init__."""
    spec = importlib.util.spec_from_file_location("_check_shipping_sidecar", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sidecar(path, with_translations=False, with_doc_relation=True, drop_table=None):
    conn = sqlite3.connect(str(path))
    try:
        doc_cols = "pgpid INTEGER PRIMARY KEY, description TEXT"
        if with_doc_relation:
            doc_cols += ", doc_relation TEXT"
        conn.execute("CREATE TABLE documents (%s)" % doc_cols)
        for table in ("document_sources", "document_footnotes", "document_fragments"):
            conn.execute("CREATE TABLE %s (id INTEGER PRIMARY KEY)" % table)
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        if with_translations:
            conn.execute(
                "CREATE TABLE pgp_translations (pgpid INTEGER PRIMARY KEY,"
                " description_he TEXT)"
            )
            conn.execute("INSERT INTO pgp_translations VALUES (1, 'x')")
        if drop_table:
            conn.execute("DROP TABLE %s" % drop_table)
        conn.commit()
    finally:
        conn.close()
    return str(path)


def test_a_clean_sidecar_ships(guard, tmp_path):
    assert guard.check_sidecar(_sidecar(tmp_path / "pgp.db")) == []


def test_a_sidecar_carrying_the_withheld_translations_is_refused(guard, tmp_path):
    """The exact state produced by scripts/restore_pgp_translations.py."""
    problems = guard.check_sidecar(_sidecar(tmp_path / "pgp.db", with_translations=True))
    assert problems
    assert any("pgp_translations" in p for p in problems)


def test_the_override_is_explicit(guard, tmp_path):
    path = _sidecar(tmp_path / "pgp.db", with_translations=True)
    assert guard.check_sidecar(path, allow_withheld=True) == []


def test_a_pre_1_1_0_sidecar_is_refused(guard, tmp_path):
    """No documents.doc_relation means 891 translations would ship labelled as
    transcriptions -- the defect PR #357 exists to fix."""
    problems = guard.check_sidecar(_sidecar(tmp_path / "pgp.db", with_doc_relation=False))
    assert problems
    assert any("doc_relation" in p for p in problems)
    # ...but an emergency build must still be possible, knowingly.
    assert guard.check_sidecar(
        _sidecar(tmp_path / "other.db", with_doc_relation=False), allow_stale_schema=True
    ) == []


def test_a_missing_core_table_is_refused(guard, tmp_path):
    problems = guard.check_sidecar(
        _sidecar(tmp_path / "pgp.db", drop_table="document_fragments")
    )
    assert any("document_fragments" in p for p in problems)


def test_a_missing_sidecar_is_refused_and_no_stub_is_created(guard, tmp_path):
    """A bare sqlite3.connect() on a missing path creates a 0-byte file that later reads
    as 'no such table'."""
    missing = tmp_path / "nope.db"
    assert guard.check_sidecar(str(missing))
    assert not missing.exists()


def test_a_non_database_is_refused_rather_than_raising(guard, tmp_path):
    junk = tmp_path / "pgp.db"
    junk.write_bytes(b"this is not a database")
    assert guard.check_sidecar(str(junk))


def _live_bat_lines():
    """build_app.bat's executable lines, with REM comments dropped.

    Comments matter here: an earlier version of this test asserted only that the string
    'check_shipping_sidecar.py' appeared somewhere in the file, which a commented-out call
    satisfies perfectly. Mutating the real call to 'REM python scripts\\...' left the test
    green -- so the test was checking that the guard is *mentioned*, not that it *runs*.
    """
    lines = []
    for raw in BUILD_BAT.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.upper().startswith("REM") or stripped.startswith("::"):
            continue
        lines.append(stripped)
    return lines


def test_build_app_bat_actually_runs_the_guard():
    """A guard the build does not call is decoration."""
    lines = _live_bat_lines()
    calls = [i for i, ln in enumerate(lines) if "check_shipping_sidecar.py" in ln]
    assert calls, "build_app.bat must INVOKE the guard, not merely mention it"

    # It must run after the WAL checkpoint, or it would inspect rows still sitting in a
    # journal and could pass a sidecar whose real contents differ.
    checkpoints = [i for i, ln in enumerate(lines) if "checkpoint_sidecars.py" in ln]
    assert checkpoints, "the WAL checkpoint must also be a live call"
    assert checkpoints[0] < calls[0]

    # ...and before anything is packaged.
    builds = [i for i, ln in enumerate(lines) if "PyInstaller" in ln]
    assert builds, "build_app.bat must still invoke PyInstaller"
    assert calls[0] < builds[0]


def test_the_guards_failure_stops_the_build():
    """Without the errorlevel check the build would carry on and ship it anyway."""
    lines = _live_bat_lines()
    call = next(i for i, ln in enumerate(lines) if "check_shipping_sidecar.py" in ln)
    build = next(i for i, ln in enumerate(lines) if "PyInstaller" in ln)
    between = lines[call + 1:build]
    assert any("errorlevel 1" in ln and "exit" in ln for ln in between), (
        "a non-zero exit from check_shipping_sidecar.py must abort build_app.bat"
    )


def test_the_withholding_decision_is_recorded_where_it_is_enforced(guard):
    """Reversing the decision should be a one-line edit in an obvious place, with the
    reason attached -- not a silent change of behaviour somewhere else."""
    assert "pgp_translations" in guard.WITHHELD_TABLES
    assert "PGP_TRANSLATION_QUALITY.md" in guard.WITHHELD_TABLES["pgp_translations"]
