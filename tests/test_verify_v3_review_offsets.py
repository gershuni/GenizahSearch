# -*- coding: utf-8 -*-
"""The offset verifier must FAIL on a wrong offset -- proven, not assumed.

A gate nobody has watched fail is not a gate. Reading
`scripts/verify_v3_review_offsets.py` cannot tell you whether it would notice a
bad offset; only putting a bad offset in front of it can. So each test here
breaks exactly one stored coordinate, runs the verifier AS A PROCESS, and
asserts both a non-zero exit AND the specific diagnostic for the row and side it
broke -- a bare non-zero exit could be an unrelated crash.

The mutations are chosen to defeat the two ways such a check goes vacuous:

  * `test_it_fails_when_a_start_lands_on_another_retained_letter` moves a start
    onto `ref_char_end - 1`, which by construction IS the position of a
    retained Hebrew letter. A naive +/-1 nudge could land in stripped
    whitespace and re-normalize to the same letters, so a checker that only
    compared normalized text would stay green; this one must not.
  * `test_it_fails_when_two_rows_swap_their_loci` swaps two rows' coordinates
    within one witness. Both spans remain individually valid positions in the
    same file -- only the pairing with the text is wrong, which is what a
    duplicated passage would look like.

`test_the_verifier_does_not_import_the_code_it_checks` is the independence
guard: if the verifier ever starts calling the builder's mapping helpers, a bug
in the map would confirm itself and every test above would still pass.

WHY THESE RUN AGAINST A FIXTURE (changed 2026-09-09)
----------------------------------------------------
They used to copy the real `discovery-v5-REVIEW.db` -- 3.45 GB, 519,382 rows --
once per mutation and verify every row of it. Measured: 3,396 seconds for six
tests, **66.5% of the whole non-GUI suite's module time**, and paid entirely by
the owner: the file is `skipif(DB is None)`, so CI (which has no review DB)
skipped it for free while the one machine with the data spent 57 minutes on it
every run.

The mutation matrix now runs against `tests/review_offsets_fixture.py`, whose
expected coordinates are hand-specified and independent of both the builder and
the verifier. Same verifier, same subprocess, same exit-code assertions, same
three mutation classes -- about a second instead of 57 minutes.

**This is a cadence change, not a free lunch, and the boundary matters:** a small
mutation matrix proves the verifier REJECTS bad coordinates. It does not prove
the 519,382 production rows are right. That check did not go away -- it moved to
`test_the_real_review_artifact_verifies_clean` below (marked `slow`, so it is
deselected from routine runs but still runnable here) and, for real enforcement,
to `scripts/verify_review_artifact.py`, which FAILS on missing data instead of
skipping and is meant to run nightly and before any artifact is promoted.
"""
from __future__ import annotations

import ast
import json
import os
import shutil
import sqlite3
import subprocess
import sys

import pytest

from tests.review_offsets_fixture import build as build_fixture

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VERIFIER = os.path.join(REPO, "scripts", "verify_v3_review_offsets.py")
KEYS = os.path.join(os.path.expanduser("~"), ".genizah-private",
                    "sourcekeys.json")

# Candidate DBs, newest smoke first: any of them exercises the same code paths.
_CANDIDATES = [
    os.path.join(REPO, "discovery_data", "discovery-v5-REVIEW.db"),
]
_SCRATCH = os.environ.get("CLAUDE_SCRATCHPAD")
if _SCRATCH:
    _CANDIDATES += [os.path.join(_SCRATCH, "SMOKE2-v5.db"),
                    os.path.join(_SCRATCH, "SMOKE-v5.db")]


def _find_db():
    for p in _CANDIDATES:
        if os.path.exists(p):
            try:
                con = sqlite3.connect("file:%s?mode=ro" % p, uri=True)
                v = con.execute("SELECT value FROM meta WHERE key='schema'"
                                ).fetchone()
                has = con.execute(
                    "SELECT COUNT(*) FROM review_row "
                    "WHERE ref_provenance_status='ok' "
                    "AND ref_char_start IS NOT NULL").fetchone()[0]
                con.close()
                if v and v[0].endswith("/2") and has:
                    return p
            except sqlite3.Error:
                continue
    return None


DB = _find_db()
needs_db = pytest.mark.skipif(DB is None,
                              reason="no schema-v2 review DB with offsets built")

# The `slow` marker alone does NOT deselect anything here: pyproject.toml
# deliberately leaves the default selection unfiltered, and CI's own `tests` job
# runs `-m "not gui and not render_smoke and not atlas_bake"` -- `slow` is not in
# that list. Verified by running it: `pytest tests/test_verify_v3_review_offsets.py`
# still spent >10 minutes in the real-data test. So the expensive check carries an
# EXPLICIT opt-in as well, and `scripts/verify_review_artifact.py` is what sets it.
_REAL_OPT_IN = "GENIZAH_VERIFY_REAL_ARTIFACT"
needs_opt_in = pytest.mark.skipif(
    os.environ.get(_REAL_OPT_IN) != "1",
    reason="set %s=1 (or run scripts/verify_review_artifact.py) to verify the "
           "real 3.45 GB artifact; it takes ~9.5 minutes" % _REAL_OPT_IN)


# ---------------------------------------------------------------------------
# The fixture the fast mutation matrix runs against
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fixture_db(tmp_path_factory):
    """A few rows over a few characters, built once for this module."""
    return build_fixture(tmp_path_factory.mktemp("review_offsets"))


def _run(db, sourcekeys=None, corpus=None):
    """Run the verifier AS A PROCESS -- the exit code is the thing under test."""
    env = dict(os.environ)
    env.setdefault("PYTHONUTF8", "1")
    cmd = [sys.executable, "-X", "utf8", VERIFIER, "--db", db, "--all"]
    if sourcekeys:
        cmd += ["--sourcekeys", sourcekeys]
    elif os.path.exists(KEYS):
        cmd += ["--sourcekeys", KEYS]
    if corpus:
        cmd += ["--corpus-file", corpus]
    return subprocess.run(cmd, capture_output=True, text=True, env=env,
                          encoding="utf-8", errors="replace")


def _run_fixture(fx, db=None):
    return _run(db or fx["db"], sourcekeys=fx["sourcekeys"], corpus=fx["corpus"])


def _mutated(fx, tmp_path, name, *statements):
    """A disposable copy of the fixture DB with `statements` applied."""
    dst = str(tmp_path / name)
    shutil.copy(fx["db"], dst)
    con = sqlite3.connect(dst)
    for sql, params in statements:
        cur = con.execute(sql, params)
        assert cur.rowcount > 0, (
            "mutation changed no rows -- it would prove nothing: %s" % sql)
    con.commit()
    con.close()
    return dst


def _masked_witness_rows(con):
    """Rows whose witness is a TRANSFORMED (cleaned) corpus -- the ones whose
    offsets pass through a substitution, so the ones worth mutating."""
    return con.execute("""
        SELECT r.evidence_id, r.ref_char_start, r.ref_char_end, r.witness_id
        FROM review_row r
        JOIN reference_witness rw ON rw.witness_id = r.witness_id
        JOIN source_file sf ON sf.id = rw.source_file_id
        WHERE r.ref_provenance_status='ok' AND r.ref_char_start IS NOT NULL
          AND sf.kind IN ('M','RS')
          AND r.ref_char_end > r.ref_char_start + 1
        ORDER BY r.evidence_id LIMIT 40""").fetchall()


# ---------------------------------------------------------------------------
# Baseline -- without this, every failure below could just mean a broken
# verifier or a broken fixture rather than a detected mutation.
# ---------------------------------------------------------------------------

def test_it_passes_on_the_unmutated_fixture(fixture_db):
    res = _run_fixture(fixture_db)
    assert res.returncode == 0, res.stdout[-4000:] + res.stderr[-2000:]
    assert "TOTAL FAILURES: 0" in res.stdout
    # Not vacuous: it must actually have examined rows on BOTH sides. A
    # verifier that checked nothing would also report zero failures.
    assert "reference side: 2 checked, 2 ok" in res.stdout, res.stdout
    assert "manuscript side: 2 ok, 0 bad" in res.stdout, res.stdout


def test_the_fixture_offsets_are_what_the_layout_says():
    """Guards the fixture itself, independently of the verifier.

    If this file's hand-written character table and the DB it builds ever drift
    apart, every mutation test above starts measuring the drift instead of the
    verifier.
    """
    from tests import review_offsets_fixture as fx
    # 'בגד' is stream[1:4]; the stream begins at character 10, so [11, 14).
    assert fx.expected_ref_offsets(1, 4) == (11, 14)
    assert fx.SOURCE_TEXT[11:14] == "בגד"
    # The manuscript spans must slice the corpus text to exactly ms_match.
    assert fx.CORPUS_TEXT[10:15] == "אבגדה"
    assert fx.CORPUS_TEXT[20:25] == "וזחטי"


# ---------------------------------------------------------------------------
# The three mutation classes
# ---------------------------------------------------------------------------

def test_it_fails_when_a_start_lands_on_another_retained_letter(
        fixture_db, tmp_path):
    con = sqlite3.connect(fixture_db["db"])
    rows = _masked_witness_rows(con)
    con.close()
    assert rows, "fixture lost its transformed-corpus rows"
    eid, _a, b, _w = rows[0]
    # b-1 IS a retained letter position (ref_char_end == pos[last]+1).
    dst = _mutated(fixture_db, tmp_path, "mutated.db",
                   ("UPDATE review_row SET ref_char_start=? WHERE evidence_id=?",
                    (b - 1, eid)))
    res = _run_fixture(fixture_db, dst)
    assert res.returncode != 0, (
        "a start moved onto a different retained letter went UNDETECTED:\n"
        + res.stdout[-4000:])
    assert "TOTAL FAILURES: 0" not in res.stdout
    # The RIGHT failure, on the right row -- a non-zero exit alone could be an
    # unrelated crash.
    assert "!= oracle" in res.stdout, res.stdout[-2000:]
    assert eid[:16] in res.stdout, res.stdout[-2000:]


def test_it_fails_when_two_rows_swap_their_loci(fixture_db, tmp_path):
    con = sqlite3.connect(fixture_db["db"])
    rows = _masked_witness_rows(con)
    con.close()
    pair = None
    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            if rows[i][3] == rows[j][3] and rows[i][1] != rows[j][1]:
                pair = (rows[i], rows[j])
                break
        if pair:
            break
    assert pair is not None, "fixture lost its same-witness pair to swap"
    (e1, a1, b1, _), (e2, a2, b2, _) = pair
    dst = _mutated(
        fixture_db, tmp_path, "swapped.db",
        ("UPDATE review_row SET ref_char_start=?, ref_char_end=? "
         "WHERE evidence_id=?", (a2, b2, e1)),
        ("UPDATE review_row SET ref_char_start=?, ref_char_end=? "
         "WHERE evidence_id=?", (a1, b1, e2)))
    res = _run_fixture(fixture_db, dst)
    assert res.returncode != 0, (
        "two rows pointing at each other's passage went UNDETECTED:\n"
        + res.stdout[-4000:])
    assert "!= oracle" in res.stdout, res.stdout[-2000:]


def test_it_fails_when_a_manuscript_offset_moves(fixture_db, tmp_path):
    con = sqlite3.connect(fixture_db["db"])
    row = con.execute(
        "SELECT evidence_id, file_char_start FROM review_row "
        "WHERE ms_provenance_status='ok' AND file_char_start IS NOT NULL "
        "ORDER BY evidence_id LIMIT 1").fetchone()
    con.close()
    assert row is not None, "fixture lost its manuscript-side offsets"
    dst = _mutated(
        fixture_db, tmp_path, "ms.db",
        ("UPDATE review_row SET file_char_start=? WHERE evidence_id=?",
         (row[1] + 1, row[0])))
    res = _run_fixture(fixture_db, dst)
    assert res.returncode != 0, (
        "a shifted manuscript offset went UNDETECTED:\n" + res.stdout[-4000:])
    # Specifically the MANUSCRIPT side, not some reference-side accident.
    assert "MS FAIL" in res.stdout, res.stdout[-2000:]


# ---------------------------------------------------------------------------
# Independence and secrecy guards -- unchanged, and deliberately unconditional
# ---------------------------------------------------------------------------

def test_the_verifier_does_not_import_the_code_it_checks():
    """INDEPENDENCE. The verifier must not reach the builder's own mapping
    helpers -- a shared implementation would make every check above
    self-confirming."""
    tree = ast.parse(open(VERIFIER, encoding="utf-8").read())
    banned_modules = {"normalize", "msource_clean", "gen2_clean_streams",
                      "build_v3_review_db", "shared.discovery_locus"}
    banned_names = {"norm_stream", "sub_offset_preserving", "compose_offsets",
                    "regen_stream_with_offsets", "regen_body_with_offsets",
                    "clean_m_with_offsets", "clean_m_body_with_offsets",
                    "seg3", "project_span"}
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for al in node.names:
                if al.name.split(".")[0] in banned_modules:
                    found.append(al.name)
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] in banned_modules:
                found.append(node.module)
            for al in node.names:
                if al.name in banned_names:
                    found.append(al.name)
    assert not found, ("the verifier imports the machinery it is supposed to "
                       "check independently: %s" % sorted(set(found)))


def test_the_key_file_is_not_inside_the_repository():
    """The id->path map names the restricted corpora. If it ever lands inside
    the repo it can be committed or swept into a handoff."""
    if not os.path.exists(KEYS):
        pytest.skip("no key file on this machine")
    assert not os.path.abspath(KEYS).startswith(os.path.abspath(REPO) + os.sep)
    json.load(open(KEYS, encoding="utf-8"))       # must be readable JSON


# ---------------------------------------------------------------------------
# The real artifact -- relocated, not deleted
# ---------------------------------------------------------------------------

@pytest.mark.slow
@needs_db
@needs_opt_in
def test_the_real_review_artifact_verifies_clean():
    """Every row of the actual review DB, against the actual sources.

    This is the check the mutation matrix above does NOT perform. It measured
    ~9.5 minutes on its own, and this file used to pay it four times over, so it
    is behind BOTH the `slow` marker and an explicit opt-in:

        GENIZAH_VERIFY_REAL_ARTIFACT=1 pytest tests/test_verify_v3_review_offsets.py -k real

    Two guards rather than one because the marker alone deselects nothing in
    this repo's default selection -- see `_REAL_OPT_IN` above.

    Do not treat a green routine suite as evidence that this passed; on a
    routine run it does not run at all. The enforcing path is
    `scripts/verify_review_artifact.py`, which FAILS rather than skips when the
    data or keys are absent, and which is meant to run nightly and before any
    rebuilt artifact is promoted.
    """
    res = _run(DB)
    assert res.returncode == 0, res.stdout[-4000:] + res.stderr[-2000:]
    assert "TOTAL FAILURES: 0" in res.stdout
