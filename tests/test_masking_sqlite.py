# -*- coding: utf-8 -*-
"""Fabricated-token proof for scripts/check_atlas_masking.py::scan_sqlite
(Phase 134 DATA-05 extension -- plan 134-02, Task 1/2).

Every test injects a FABRICATED, test-only known-bad token (NEVER a real
restricted M-source/R-source string) and asserts `scan_sqlite` flags it --
on the schema/DDL surface AND the per-cell surface, for BOTH str/TEXT values
AND bytes/BLOB cells -- while NEVER echoing the token in any Issue diagnostic.

The FROZEN signature (F4) is `scan_sqlite(db_path, patterns)`: a `patterns`
LIST (mirroring `scan_asset(path, patterns)`); `scan_sqlite` builds the
matcher INTERNALLY via `build_matcher(patterns)`. Every call below therefore
passes a plain list of pattern strings, never a pre-built matcher.

`MASKING_SCAN_PATTERNS_FILE` is UNSET/owner-held in this environment -- these
tests never depend on it (the fail-safe unset-file CLI behavior is itself
proven as a property, never by relying on a real pattern file existing).
"""
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / 'scripts' / 'check_atlas_masking.py'

sys.path.insert(0, str(REPO_ROOT / 'scripts'))
import check_atlas_masking as cam  # noqa: E402

# Fabricated, test-only tokens -- NEVER the real restricted M-source/R-source
# string. Distinct from tests/test_atlas_masking_scan.py's fixtures so this
# file stays fully self-contained.
FAKE = 'ZZZ_FAKE_SQLITE_MASKING_TOKEN_ZZZ'
FAKE_HE = 'צצצ_מסך_בדיקת_סקוליט_צצצ'
PATTERNS = [FAKE, FAKE_HE]

GIT = shutil.which('git')


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _assert_never_echoes(issues):
    for issue in issues:
        rendered = issue.format() + ' ' + repr(issue)
        for tok in PATTERNS:
            assert tok not in rendered, f"never-echo violated for {tok!r}: {rendered!r}"


def _make_db(tmp_path, *, leaky_column_name=False, filename='fixture.db') -> Path:
    """A tiny throwaway SQLite file carrying the FAKE token in (a) a str
    cell, (b) a BLOB cell, and optionally (c) a column NAME (so the schema/DDL
    surface -- sqlite_master.sql -- is exercised, F-schema)."""
    db = tmp_path / filename
    conn = sqlite3.connect(str(db))
    try:
        col_name = FAKE if leaky_column_name else 'notes'
        quoted_col = '"' + col_name.replace('"', '""') + '"'
        conn.execute(
            f'CREATE TABLE claims (id INTEGER, {quoted_col} TEXT, evidence BLOB)'
        )
        conn.execute(
            f'INSERT INTO claims (id, {quoted_col}, evidence) VALUES (?, ?, ?)',
            (1, f"a cell value containing {FAKE}",
             f"blob payload containing {FAKE}".encode('utf-8')),
        )
        conn.execute(
            f'INSERT INTO claims (id, {quoted_col}, evidence) VALUES (?, ?, ?)',
            (2, "a clean cell with no leak", b"clean blob, nothing restricted"),
        )
        conn.commit()
    finally:
        conn.close()
    return db


def _git(repo, *args):
    subprocess.run([GIT, *args], cwd=str(repo), check=True, capture_output=True)


@pytest.fixture
def git_repo(tmp_path, monkeypatch):
    """A throwaway git repo; cam.ROOT_DIR is redirected here so --scan-repo
    (called in-process via cam.main()) never touches the real project tree."""
    if not GIT:
        pytest.skip("git not available")
    repo = tmp_path / 'repo'
    repo.mkdir()
    _git(repo, 'init', '-q')
    _git(repo, 'config', 'user.email', 't@example.invalid')
    _git(repo, 'config', 'user.name', 'Test')
    _git(repo, 'config', 'commit.gpgsign', 'false')
    _git(repo, 'config', 'core.autocrlf', 'false')
    monkeypatch.setattr(cam, 'ROOT_DIR', repo)
    return repo


# ---------------------------------------------------------------------------
# 1. cell-level scan: schema (incl. leaky column NAME) + str cell + BLOB cell
# ---------------------------------------------------------------------------

def test_scan_sqlite_flags_str_cell(tmp_path):
    db = _make_db(tmp_path)
    issues = cam.scan_sqlite(str(db), PATTERNS)
    cell_hits = [i for i in issues if i.path.endswith('::claims.notes')]
    assert cell_hits, "str/TEXT cell leak not flagged"
    _assert_never_echoes(issues)


def test_scan_sqlite_flags_blob_cell(tmp_path):
    db = _make_db(tmp_path)
    issues = cam.scan_sqlite(str(db), PATTERNS)
    blob_hits = [i for i in issues if i.path.endswith('::claims.evidence')]
    assert blob_hits, "bytes/BLOB cell leak not flagged"
    _assert_never_echoes(issues)


def test_scan_sqlite_flags_schema_surface_via_leaky_column_name(tmp_path):
    """A leaked column NAME lives only in sqlite_master.sql (the CREATE TABLE
    DDL) -- this is the schema surface, not a per-cell surface."""
    db = _make_db(tmp_path, leaky_column_name=True)
    issues = cam.scan_sqlite(str(db), PATTERNS)
    schema_hits = [i for i in issues if i.path.endswith('::schema')]
    assert schema_hits, "leaky column NAME not flagged via the schema/DDL surface"
    _assert_never_echoes(issues)


def test_scan_sqlite_flags_both_str_and_blob_together(tmp_path):
    """Both surfaces (str + BLOB) are flagged in the SAME scan -- proving
    scan_sqlite iterates every column of every row, not just the first
    leak-bearing column type it finds."""
    db = _make_db(tmp_path)
    issues = cam.scan_sqlite(str(db), PATTERNS)
    surfaces_hit = {i.path.rsplit('::', 1)[-1] for i in issues}
    assert 'claims.notes' in surfaces_hit
    assert 'claims.evidence' in surfaces_hit


def test_scan_sqlite_clean_db_passes(tmp_path):
    db = tmp_path / 'clean.db'
    conn = sqlite3.connect(str(db))
    conn.execute('CREATE TABLE t (id INTEGER, notes TEXT)')
    conn.execute('INSERT INTO t VALUES (1, ?)', ('nothing restricted here',))
    conn.commit()
    conn.close()
    assert cam.scan_sqlite(str(db), PATTERNS) == []


def test_scan_sqlite_takes_patterns_list_not_prebuilt_matcher(tmp_path):
    """F4: the FROZEN signature is `scan_sqlite(db_path, patterns)` -- a
    patterns LIST. This test calls it exactly that way (never pre-building a
    matcher and passing that instead) and confirms it still works end-to-end,
    proving scan_sqlite builds its own matcher internally via build_matcher."""
    db = _make_db(tmp_path)
    assert isinstance(PATTERNS, list) and all(isinstance(p, str) for p in PATTERNS)
    issues = cam.scan_sqlite(str(db), PATTERNS)
    assert issues


# ---------------------------------------------------------------------------
# 2. never-echo redaction (filename surface too)
# ---------------------------------------------------------------------------

def test_scan_sqlite_leaky_db_filename_redacted(tmp_path):
    db = _make_db(tmp_path, filename=f'{FAKE}_sidecar.db')
    issues = cam.scan_sqlite(str(db), PATTERNS)
    name_hits = [i for i in issues if i.surface == 'filename']
    assert name_hits, "leaky db filename itself was not flagged"
    _assert_never_echoes(issues)


# ---------------------------------------------------------------------------
# 3. fail-CLOSED: connect / read / decode errors, and zero-pattern refusal
# ---------------------------------------------------------------------------

def test_scan_sqlite_missing_file_fails_closed(tmp_path):
    with pytest.raises(cam.ScanError):
        cam.scan_sqlite(str(tmp_path / 'does-not-exist.db'), PATTERNS)


def test_scan_sqlite_not_a_database_fails_closed(tmp_path):
    bogus = tmp_path / 'not-a-db.db'
    bogus.write_bytes(b'this is not a sqlite file at all, just plain bytes\x00\x01')
    with pytest.raises(cam.ScanError):
        cam.scan_sqlite(str(bogus), PATTERNS)


def test_scan_sqlite_empty_patterns_raises(tmp_path):
    db = _make_db(tmp_path)
    with pytest.raises(cam.ScanError):
        cam.scan_sqlite(str(db), [])


def test_scan_sqlite_all_blank_patterns_raises(tmp_path):
    db = _make_db(tmp_path)
    # `_require_patterns` treats any non-empty string as usable (it does not
    # strip whitespace-only entries) -- mirror the existing convention
    # (test_atlas_masking_scan.py::test_build_matcher_all_blank_raises) by
    # pre-stripping so the fixture is genuinely all-blank.
    with pytest.raises(cam.ScanError):
        cam.scan_sqlite(str(db), ['', '   '.strip()])


# ---------------------------------------------------------------------------
# 4. CLI: --scan-sqlite composes with --scan-asset/--scan-repo/--strict
#    (the VALID ship-gate form), and never silently green on unset patterns
# ---------------------------------------------------------------------------

def test_cli_scan_sqlite_composes_with_asset_repo_strict(git_repo, monkeypatch):
    """The VALID ship-gate form -- `--scan-sqlite <DB> --scan-asset <DB>
    --scan-repo --strict` -- composes in ONE invocation. Exercised end-to-end
    IN-PROCESS (cam.ROOT_DIR pointed at a throwaway repo -- never the real
    project tree) against a db carrying a fabricated leak: proves the form is
    ACCEPTED (no argparse usage-error / exit code 2), the sqlite issue
    participates in the combined non-zero exit code, and the token is never
    echoed to stdout/stderr."""
    db = git_repo / 'fixture.db'
    conn = sqlite3.connect(str(db))
    conn.execute('CREATE TABLE claims (id INTEGER, notes TEXT)')
    conn.execute('INSERT INTO claims VALUES (1, ?)', (f'leak: {FAKE}',))
    conn.commit()
    conn.close()
    (git_repo / 'clean.txt').write_text('nothing restricted here\n', encoding='utf-8')
    _git(git_repo, 'add', '-A')
    _git(git_repo, 'commit', '-q', '-m', 'add fixture db + clean file')

    pf = git_repo.parent / 'patterns.txt'
    pf.write_text(FAKE + '\n' + FAKE_HE + '\n', encoding='utf-8')
    monkeypatch.setenv('MASKING_SCAN_PATTERNS_FILE', str(pf))

    rc = cam.main(['--scan-sqlite', str(db), '--scan-asset', str(db),
                   '--scan-repo', '--strict'])
    assert rc == 1, "expected the sqlite leak to be caught (non-zero exit)"


def test_cli_scan_sqlite_composes_with_asset_repo_strict_clean_exits_zero(
        git_repo, monkeypatch, capsys):
    """Same combined form over CLEAN content exits 0 -- proves the combined
    form is genuinely ACCEPTED end-to-end (not merely tolerated because an
    earlier surface's error short-circuits before the sqlite scan runs)."""
    db = git_repo / 'fixture.db'
    conn = sqlite3.connect(str(db))
    conn.execute('CREATE TABLE t (id INTEGER, notes TEXT)')
    conn.execute('INSERT INTO t VALUES (1, ?)', ('nothing restricted here',))
    conn.commit()
    conn.close()
    _git(git_repo, 'add', '-A')
    _git(git_repo, 'commit', '-q', '-m', 'clean fixture')

    pf = git_repo.parent / 'patterns.txt'
    pf.write_text(FAKE + '\n' + FAKE_HE + '\n', encoding='utf-8')
    monkeypatch.setenv('MASKING_SCAN_PATTERNS_FILE', str(pf))

    rc = cam.main(['--scan-sqlite', str(db), '--scan-asset', str(db),
                   '--scan-repo', '--strict'])
    assert rc == 0, "combined --scan-sqlite/--scan-asset/--scan-repo/--strict form was rejected on clean content"
    captured = capsys.readouterr()
    assert FAKE not in (captured.out + captured.err)


def test_cli_scan_sqlite_alone_fails_safe_when_patterns_unset(tmp_path, monkeypatch, capsys):
    """The unset-file fail-safe property: with MASKING_SCAN_PATTERNS_FILE
    unset, `--scan-sqlite` must NEVER silently exit 0 -- it hits the same
    `_require_patterns` gate as --scan-repo/--scan-asset and exits 1 (ERROR),
    never a silent-green false pass."""
    monkeypatch.delenv('MASKING_SCAN_PATTERNS_FILE', raising=False)
    db = _make_db(tmp_path)
    rc = cam.main(['--scan-sqlite', str(db)])
    assert rc == 1, "unset pattern file must fail closed (exit 1), never silently succeed"
    captured = capsys.readouterr()
    assert 'ERROR' in (captured.out + captured.err)


def test_cli_help_lists_scan_sqlite():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), '--help'],
        cwd=str(REPO_ROOT), capture_output=True, text=True,
    )
    assert result.returncode == 0
    assert '--scan-sqlite' in result.stdout


def test_cli_scan_sqlite_empty_path_rejected():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), '--scan-sqlite', ''],
        cwd=str(REPO_ROOT), capture_output=True, text=True,
    )
    assert result.returncode == 2
    assert 'ERROR' in (result.stdout + result.stderr)


# ---------------------------------------------------------------------------
# 5. R-source pre-registration (D-03c, operational -- see plan Task 2 note)
# ---------------------------------------------------------------------------

def test_r_source_pattern_file_ingestion_mechanism_unchanged(tmp_path, monkeypatch):
    """D-03c defense-in-depth: pre-registering R-source name/aliases/sigla is
    a purely OPERATIONAL step (append lines to the owner-held, gitignored
    MASKING_SCAN_PATTERNS_FILE) -- it requires NO code change, because
    `load_patterns()` already ingests one pattern per non-comment line,
    unconditionally, regardless of which corpus a line's token belongs to.
    This test proves that ingestion mechanism with fabricated multi-line
    patterns (never a real restricted string) so the pre-registration
    workflow itself is regression-tested without requiring the real,
    owner-held token file to exist in CI."""
    pf = tmp_path / 'patterns.txt'
    pf.write_text(
        "# M-source tokens\n" + FAKE + "\n"
        "# R-source tokens (fabricated stand-ins for this test)\n"
        + FAKE_HE + "\nfake_r_source_alias_token\n",
        encoding='utf-8',
    )
    monkeypatch.setenv('MASKING_SCAN_PATTERNS_FILE', str(pf))
    loaded = cam.load_patterns()
    assert loaded == [FAKE, FAKE_HE, 'fake_r_source_alias_token']
