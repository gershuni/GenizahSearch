# -*- coding: utf-8 -*-
"""``stale_checkpoint_ids`` must catch a checkpoint the database cannot confirm.

The checkpoint file records only pgpids, never translated text, and the batch loop skips every
id it names. ``scripts/export_pgp_sidecar.py`` deletes pgp.db and recreates it *without*
``pgp_translations``, so after a rebuild every id in an old checkpoint points at a row that no
longer exists. Resuming then skips those documents forever and prints a successful summary --
which is exactly how the table stayed missing from 2026-04-22 to 2026-09-21.

This guard turns that into a stop condition, so it is worth a test that can fail: the first run
against the real 2026-09 checkpoint found 221 such ids.

The last two tests cover Codex's round-1 findings on the same guard: a refusal has to reach the
process exit code, or a wrapper reads it as a successful run; and a blank API answer has to be
retried rather than accepted and then recorded as a failure the retries never fought.
"""
from __future__ import annotations

import importlib.util
import json
import pathlib
import sqlite3
import threading
import time

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _load_script():
    """Import the script by path -- ``scripts/`` is a flat namespace package with no __init__."""
    path = REPO_ROOT / "scripts" / "translate_pgp_descriptions.py"
    spec = importlib.util.spec_from_file_location("_translate_pgp_descriptions", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def script():
    return _load_script()


def _make_db(path: pathlib.Path, *, with_table: bool, pgpids=()) -> str:
    conn = sqlite3.connect(str(path))
    try:
        if with_table:
            conn.execute(
                "CREATE TABLE pgp_translations ("
                " pgpid INTEGER PRIMARY KEY, description_he TEXT, document_type_he TEXT)"
            )
            conn.executemany(
                "INSERT INTO pgp_translations (pgpid, description_he) VALUES (?, ?)",
                [(p, "x") for p in pgpids],
            )
        else:
            conn.execute("CREATE TABLE documents (pgpid INTEGER PRIMARY KEY)")
        conn.commit()
    finally:
        conn.close()
    return str(path)


def test_empty_checkpoint_is_never_stale(script, tmp_path):
    db = _make_db(tmp_path / "pgp.db", with_table=True, pgpids=[1, 2])
    assert script.stale_checkpoint_ids(db, set()) == set()


def test_every_id_is_stale_when_the_table_is_gone(script, tmp_path):
    """The rebuild case: pgp.db exists, pgp_translations does not."""
    db = _make_db(tmp_path / "pgp.db", with_table=False)
    assert script.stale_checkpoint_ids(db, {1, 2, 3}) == {1, 2, 3}


def test_only_the_missing_ids_are_reported(script, tmp_path):
    db = _make_db(tmp_path / "pgp.db", with_table=True, pgpids=[1, 3])
    assert script.stale_checkpoint_ids(db, {1, 2, 3, 4}) == {2, 4}


def test_a_fully_confirmed_checkpoint_is_clean(script, tmp_path):
    db = _make_db(tmp_path / "pgp.db", with_table=True, pgpids=[1, 2, 3])
    assert script.stale_checkpoint_ids(db, {1, 2, 3}) == set()


def test_more_ids_than_one_sql_batch(script, tmp_path):
    """The lookup chunks at 400 ids to stay under SQLITE_MAX_VARIABLE_NUMBER."""
    present = list(range(1, 1001))
    db = _make_db(tmp_path / "pgp.db", with_table=True, pgpids=present)
    checkpoint = set(range(1, 1201))
    assert script.stale_checkpoint_ids(db, checkpoint) == set(range(1001, 1201))


def test_the_probe_does_not_write_to_the_database(script, tmp_path):
    """Opened read-only on purpose: a bare sqlite3.connect() against a missing sidecar
    creates a 0-byte stub that later reads as 'no such table' (see the nli_crossref trap)."""
    path = tmp_path / "pgp.db"
    db = _make_db(path, with_table=True, pgpids=[1])
    before = path.stat().st_mtime_ns, path.stat().st_size
    script.stale_checkpoint_ids(db, {1, 2})
    assert (path.stat().st_mtime_ns, path.stat().st_size) == before


def test_a_missing_database_is_not_silently_treated_as_clean(script, tmp_path):
    """mode=ro refuses to create the file, so a wrong --pgp-db raises instead of
    reporting an empty stale set (which would read as 'checkpoint confirmed')."""
    with pytest.raises(sqlite3.OperationalError):
        script.stale_checkpoint_ids(str(tmp_path / "does_not_exist.db"), {1})


# ---------------------------------------------------------------------------
# Codex round 1 on PR #355. Both of these were exit-code/retry blindness, not
# logic errors -- the kind a green test suite happily reports as healthy.
# ---------------------------------------------------------------------------

def _args(script, **overrides):
    """A real parsed namespace, so the test breaks if a flag is renamed."""
    argv = []
    for key, value in overrides.items():
        flag = "--" + key.replace("_", "-")
        if value is True:
            argv.append(flag)
        else:
            argv += [flag, str(value)]
    return script.parse_args(argv)


def test_a_refused_run_exits_nonzero(script, tmp_path, capsys):
    """The guard printing a diagnostic and returning 0 is how a refusal gets read as a run."""
    db = tmp_path / "pgp.db"
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("CREATE TABLE documents (pgpid INTEGER PRIMARY KEY, description TEXT, document_type TEXT)")
        conn.execute("INSERT INTO documents VALUES (1, 'a description long enough to qualify', 'Letter')")
        conn.execute("CREATE TABLE pgp_translations (pgpid INTEGER PRIMARY KEY, description_he TEXT)")
        conn.commit()
    finally:
        conn.close()

    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_text(json.dumps({"completed_ids": [1]}), encoding="utf-8")

    code = script.run_batch(_args(script, pgp_db=db, checkpoint_file=checkpoint))
    assert code == 2, "a refused run must not exit 0"
    assert "ERROR" in capsys.readouterr().out


def test_ignoring_the_stale_checkpoint_does_not_return_the_refusal_code(script, tmp_path):
    """--ignore-stale-checkpoint means 'go on', so the refusal code must not leak out of it."""
    db = tmp_path / "pgp.db"
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("CREATE TABLE documents (pgpid INTEGER PRIMARY KEY, description TEXT, document_type TEXT)")
        conn.execute("INSERT INTO documents VALUES (1, 'a description long enough to qualify', 'Letter')")
        conn.execute("CREATE TABLE pgp_translations (pgpid INTEGER PRIMARY KEY, description_he TEXT)")
        conn.commit()
    finally:
        conn.close()

    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_text(json.dumps({"completed_ids": [1]}), encoding="utf-8")

    # id 1 is both stale AND the only candidate, so with the guard waived there is nothing
    # pending and the run ends cleanly without an API call.
    code = script.run_batch(
        _args(script, pgp_db=db, checkpoint_file=checkpoint, ignore_stale_checkpoint=True)
    )
    assert code == 0


def test_a_blank_answer_is_retried(script, monkeypatch):
    """Dicta answers some inputs with '' rather than an error. Accepting that on attempt 1
    spends none of the retry budget on a response a second request may well fill in."""
    answers = ["", "   ", "\u05ea\u05e8\u05d2\u05d5\u05dd"]
    calls = []

    def fake_translate(text, prompt, direction):
        calls.append(text)
        return answers[len(calls) - 1]

    monkeypatch.setattr(script, "translate_text", fake_translate)
    monkeypatch.setattr(script.time, "sleep", lambda _s: None)

    out = script.translate_with_retry("some english", "prompt", "en2he")
    assert out == "\u05ea\u05e8\u05d2\u05d5\u05dd"
    assert len(calls) == 3, "the two blank answers should each have cost an attempt"


def test_all_blank_answers_report_failure(script, monkeypatch):
    monkeypatch.setattr(script, "translate_text", lambda *a: "")
    monkeypatch.setattr(script.time, "sleep", lambda _s: None)
    assert script.translate_with_retry("some english", "prompt", "en2he") is None


def _seed_db(path, pgpids):
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE documents (pgpid INTEGER PRIMARY KEY, description TEXT, document_type TEXT)"
        )
        conn.executemany(
            "INSERT INTO documents VALUES (?, ?, ?)",
            [(p, "an english description long enough to qualify for translation", "Letter")
             for p in pgpids],
        )
        conn.execute(
            "CREATE TABLE pgp_translations ("
            " pgpid INTEGER PRIMARY KEY, description_he TEXT, document_type_he TEXT,"
            " model_version TEXT, translated_at TEXT)"
        )
        conn.commit()
    finally:
        conn.close()
    return str(path)


def test_the_delay_throttles_the_workers_not_the_consumer(script, tmp_path, monkeypatch):
    """Codex round 3. Every task is submitted up front, so a sleep in the as_completed loop
    paces result processing on the main thread while the pool runs flat out -- no throttle at
    all. The delay has to be inside the worker, which is what this asserts by thread name."""
    db = _seed_db(tmp_path / "pgp.db", range(1, 9))
    delay = 0.01
    sleeps = []
    real_sleep = time.sleep

    def recording_sleep(seconds):
        sleeps.append((threading.current_thread().name, seconds))
        real_sleep(0)

    monkeypatch.setattr(script.time, "sleep", recording_sleep)
    monkeypatch.setattr(script, "translate_with_retry", lambda *a, **k: "\u05ea\u05e8\u05d2\u05d5\u05dd")

    code = script.run_batch(_args(
        script, pgp_db=db, checkpoint_file=tmp_path / "cp.json",
        workers=2, delay=delay, batch_size=100,
    ))
    assert code == 0

    throttle = [(name, sec) for name, sec in sleeps if sec == delay]
    assert throttle, "the delay never reached anything"
    assert not [n for n, _ in throttle if n == "MainThread"], (
        "the consumer is sleeping, so --delay paces result processing and not the API calls: "
        + repr(throttle)
    )


def test_the_sequential_path_still_throttles_on_the_main_thread(script, tmp_path, monkeypatch):
    """The counterpart: with one worker there is no pool, and the consumer IS the caller."""
    db = _seed_db(tmp_path / "pgp.db", range(1, 4))
    delay = 0.01
    sleeps = []
    real_sleep = time.sleep

    def recording_sleep(seconds):
        sleeps.append((threading.current_thread().name, seconds))
        real_sleep(0)

    monkeypatch.setattr(script.time, "sleep", recording_sleep)
    monkeypatch.setattr(script, "translate_with_retry", lambda *a, **k: "\u05ea\u05e8\u05d2\u05d5\u05dd")

    code = script.run_batch(_args(
        script, pgp_db=db, checkpoint_file=tmp_path / "cp.json",
        workers=1, delay=delay, batch_size=100,
    ))
    assert code == 0
    assert [n for n, sec in sleeps if sec == delay and n == "MainThread"], (
        "the sequential path lost its throttle: " + repr(sleeps)
    )
