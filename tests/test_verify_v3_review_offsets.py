# -*- coding: utf-8 -*-
"""The offset verifier must FAIL on a wrong offset -- proven, not assumed.

A gate nobody has watched fail is not a gate. Reading
`scripts/verify_v3_review_offsets.py` cannot tell you whether it would notice a
bad offset; only putting a bad offset in front of it can. So each test here
copies a real review DB, breaks exactly one stored coordinate, runs the verifier
AS A PROCESS, and asserts a non-zero exit.

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

These need a built review DB; they skip when none is present, and the M-source
mutations additionally need the local key file (the restricted paths live only
there, by design).
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


def _run(db):
    env = dict(os.environ)
    env.setdefault("PYTHONUTF8", "1")
    cmd = [sys.executable, "-X", "utf8", VERIFIER, "--db", db, "--all"]
    if os.path.exists(KEYS):
        cmd += ["--sourcekeys", KEYS]
    return subprocess.run(cmd, capture_output=True, text=True, env=env,
                          encoding="utf-8", errors="replace")


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


@needs_db
def test_it_passes_on_the_unmutated_db():
    """Baseline: the check is green before anything is broken. Without this the
    failures below could just mean the verifier is broken."""
    res = _run(DB)
    assert res.returncode == 0, res.stdout[-4000:] + res.stderr[-2000:]
    assert "TOTAL FAILURES: 0" in res.stdout


@needs_db
def test_it_fails_when_a_start_lands_on_another_retained_letter(tmp_path):
    if not os.path.exists(KEYS):
        pytest.skip("restricted-source key file absent; cannot resolve M/RS")
    dst = str(tmp_path / "mutated.db")
    shutil.copy(DB, dst)
    con = sqlite3.connect(dst)
    rows = _masked_witness_rows(con)
    if not rows:
        pytest.skip("no transformed-corpus rows in this DB")
    eid, a, b, _ = rows[0]
    # b-1 IS a retained letter position (ref_char_end == pos[last]+1).
    con.execute("UPDATE review_row SET ref_char_start=? WHERE evidence_id=?",
                (b - 1, eid))
    con.commit()
    con.close()
    res = _run(dst)
    assert res.returncode != 0, (
        "a start moved onto a different retained letter went UNDETECTED:\n"
        + res.stdout[-4000:])
    assert "TOTAL FAILURES: 0" not in res.stdout


@needs_db
def test_it_fails_when_two_rows_swap_their_loci(tmp_path):
    if not os.path.exists(KEYS):
        pytest.skip("restricted-source key file absent; cannot resolve M/RS")
    dst = str(tmp_path / "swapped.db")
    shutil.copy(DB, dst)
    con = sqlite3.connect(dst)
    rows = _masked_witness_rows(con)
    pair = None
    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            if rows[i][3] == rows[j][3] and rows[i][1] != rows[j][1]:
                pair = (rows[i], rows[j])
                break
        if pair:
            break
    if pair is None:
        pytest.skip("no two same-witness rows to swap in this DB")
    (e1, a1, b1, _), (e2, a2, b2, _) = pair
    con.execute("UPDATE review_row SET ref_char_start=?, ref_char_end=? "
                "WHERE evidence_id=?", (a2, b2, e1))
    con.execute("UPDATE review_row SET ref_char_start=?, ref_char_end=? "
                "WHERE evidence_id=?", (a1, b1, e2))
    con.commit()
    con.close()
    res = _run(dst)
    assert res.returncode != 0, (
        "two rows pointing at each other's passage went UNDETECTED:\n"
        + res.stdout[-4000:])


@needs_db
def test_it_fails_when_a_manuscript_offset_moves(tmp_path):
    dst = str(tmp_path / "ms.db")
    shutil.copy(DB, dst)
    con = sqlite3.connect(dst)
    row = con.execute(
        "SELECT evidence_id, file_char_start FROM review_row "
        "WHERE ms_provenance_status='ok' AND file_char_start IS NOT NULL "
        "ORDER BY evidence_id LIMIT 1").fetchone()
    if row is None:
        pytest.skip("no manuscript-side offsets in this DB")
    con.execute("UPDATE review_row SET file_char_start=? WHERE evidence_id=?",
                (row[1] + 1, row[0]))
    con.commit()
    con.close()
    res = _run(dst)
    assert res.returncode != 0, (
        "a shifted manuscript offset went UNDETECTED:\n" + res.stdout[-4000:])


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
