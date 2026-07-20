# -*- coding: utf-8 -*-
"""Load-bearing self-test for scripts/check_atlas_masking.py (D-07).

Every test injects a FABRICATED, test-only known-bad token (NEVER the real
restricted M-source string) and asserts the scan flags it while NEVER echoing
it. The suite is deliberately load-bearing across ALL the hardened surfaces:

  * the ONE semantically-complete matcher (literal, ASCII-case, Unicode NFC/NFD,
    UTF-8/UTF-16/UTF-32 encodings, URL/HTML/JS escaped forms, and MIXED
    literal+escaped forms);
  * the ACTUAL `scan_repo` path against REAL temporary git repositories that
    exercise HEAD/index/worktree/untracked divergence, gitignore exclusion,
    non-ASCII paths, embedded-newline path parsing, and leaky FILENAMES with
    redaction;
  * REAL Brotli (`.br`) payloads (decompressed + scanned);
  * the fail-CLOSED policy (git-command failure, missing blob, unreadable file,
    BOM-decode failure, empty pattern set at EVERY entry point);
  * never-echo of the matched pattern and of a leaky path.
"""
import os
import shutil
import subprocess
import sys
import unicodedata
import urllib.parse
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / 'scripts' / 'check_atlas_masking.py'

sys.path.insert(0, str(REPO_ROOT / 'scripts'))
import check_atlas_masking as cam  # noqa: E402

# Fabricated, test-only tokens -- NEVER the real restricted M-source string.
FAKE = 'ZZZ_FAKE_MASKING_TOKEN_ZZZ'
FAKE_HE = 'צצצ_מסך_בדיקה_צצצ'
FAKE_DOMAIN = 'fake-mask.invalid'
ALL_FAKES = (FAKE, FAKE_HE, FAKE_DOMAIN)

GIT = shutil.which('git')


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _assert_never_echoes(issues, extra_text=''):
    for issue in issues:
        rendered = issue.format() + ' ' + repr(issue) + ' ' + extra_text
        for tok in ALL_FAKES:
            assert tok not in rendered, f"never-echo violated for {tok!r}: {rendered!r}"


def _write_patterns_file(tmp_path, patterns) -> Path:
    p = tmp_path / 'patterns.txt'
    p.write_text('\n'.join(patterns) + '\n', encoding='utf-8')
    return p


def _run_cli(env_overrides, args) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.pop('MASKING_SCAN_PATTERNS_FILE', None)
    env.update(env_overrides)
    return subprocess.run(
        [sys.executable, str(SCRIPT)] + args,
        cwd=str(REPO_ROOT), capture_output=True, text=True, env=env,
    )


def _git(repo, *args):
    subprocess.run([GIT, *args], cwd=str(repo), check=True,
                   capture_output=True)


@pytest.fixture
def matcher():
    return cam.build_matcher([FAKE, FAKE_HE, FAKE_DOMAIN])


@pytest.fixture
def git_repo(tmp_path, monkeypatch):
    """A throwaway git repo; cam.ROOT_DIR is redirected here so scan_repo runs
    against it (both git subprocess cwd and worktree file reads)."""
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
# 1. The ONE matcher -- semantic coverage (load-bearing across every form)
# ---------------------------------------------------------------------------

def test_literal_utf8(matcher):
    issues = matcher.scan(f"a {FAKE} b".encode('utf-8'), 'fx')
    assert issues and any(i.surface == 'raw' for i in issues)
    _assert_never_echoes(issues)


def test_ascii_case_insensitive(matcher):
    issues = matcher.scan(b"xx " + FAKE.swapcase().encode('ascii') + b" yy", 'fx')
    assert issues, "arbitrary ASCII case variant was not detected"


def test_hebrew_literal(matcher):
    issues = matcher.scan(f"x {FAKE_HE} y".encode('utf-8'), 'fx')
    assert issues, "raw Hebrew UTF-8 token not detected"
    _assert_never_echoes(issues)


def test_unicode_nfd_form(matcher):
    nfd = unicodedata.normalize('NFD', FAKE_HE)
    issues = matcher.scan(f"x {nfd} y".encode('utf-8'), 'fx')
    assert issues, "NFD-normalized form not detected"


def test_utf16_bom(matcher):
    issues = matcher.scan(('﻿' + FAKE).encode('utf-16'), 'fx')
    assert issues and any(i.surface == 'decoded' for i in issues)


def test_utf32_bom(matcher):
    issues = matcher.scan(('﻿' + FAKE).encode('utf-32'), 'fx')
    assert issues and any(i.surface == 'decoded' for i in issues)


def test_utf16_no_bom_dense(matcher):
    issues = matcher.scan((FAKE * 3).encode('utf-16-le'), 'fx')
    assert issues, "BOM-less UTF-16 (dense NUL) not detected"


def test_url_encoded_full(matcher):
    data = f"q={urllib.parse.quote(FAKE, safe='')}".encode('ascii')
    assert matcher.scan(data, 'fx')


def test_url_encoded_mixed(matcher):
    # first char literal, rest percent-encoded
    mixed = FAKE[0] + urllib.parse.quote(FAKE[1:], safe='')
    assert matcher.scan(mixed.encode('ascii'), 'fx')


def test_html_entity_decimal(matcher):
    data = ('<p>' + ''.join(f'&#{ord(c)};' for c in FAKE) + '</p>').encode('ascii')
    assert matcher.scan(data, 'fx')


def test_html_entity_hex(matcher):
    data = ('<p>' + ''.join(f'&#x{ord(c):X};' for c in FAKE) + '</p>').encode('ascii')
    assert matcher.scan(data, 'fx'), "upper-case-hex HTML entity not detected"


def test_js_unicode_escape_lower_and_upper(matcher):
    lower = ('"' + ''.join(f'\\u{ord(c):04x}' for c in FAKE) + '"').encode('ascii')
    upper = ('"' + ''.join(f'\\u{ord(c):04X}' for c in FAKE) + '"').encode('ascii')
    assert matcher.scan(lower, 'fx')
    assert matcher.scan(upper, 'fx'), "upper-case-hex JS escape not detected"


def test_js_hex_escape(matcher):
    data = ('"' + ''.join(f'\\x{ord(c):02x}' for c in FAKE) + '"').encode('ascii')
    assert matcher.scan(data, 'fx')


def test_mixed_literal_and_js_escape(matcher):
    # 3 literal chars + rest JS-escaped: only the de-escape text pass catches this
    mixed = (FAKE[:3] + ''.join(f'\\u{ord(c):04x}' for c in FAKE[3:])).encode('ascii')
    issues = matcher.scan(mixed, 'fx')
    assert issues and any(i.surface == 'escape' for i in issues), (
        "mixed literal+escaped form not caught by the de-escape pass"
    )


def test_hebrew_url_encoded(matcher):
    data = f"q={urllib.parse.quote(FAKE_HE, safe='')}".encode('ascii')
    assert matcher.scan(data, 'fx')


def test_clean_content_passes(matcher):
    assert matcher.scan(b"ordinary content, nothing restricted here.\n", 'fx') == []


def test_never_echo_in_issue(matcher):
    issues = matcher.scan(f"leak {FAKE}".encode('utf-8'), 'fx')
    assert issues
    _assert_never_echoes(issues)


def test_dense_escape_introducers_wholefile(matcher):
    """A blob dense in escape introducers de-escapes whole-file (not windowed)
    and still catches a mixed form buried in the noise."""
    noise = b'&#65;' * (cam._DENSE_INTRO_LIMIT + 50)
    mixed = (FAKE[:2] + ''.join(f'\\u{ord(c):04x}' for c in FAKE[2:])).encode('ascii')
    assert matcher.scan(noise + mixed + noise, 'fx')


# ---------------------------------------------------------------------------
# 2. Zero-pattern enforcement at EVERY public entry point (HIGH-9)
# ---------------------------------------------------------------------------

def test_build_matcher_empty_raises():
    with pytest.raises(cam.ScanError):
        cam.build_matcher([])


def test_build_matcher_all_blank_raises():
    with pytest.raises(cam.ScanError):
        cam.build_matcher(['', '   '.strip()])


def test_scan_repo_empty_raises():
    with pytest.raises(cam.ScanError):
        cam.scan_repo([])


def test_scan_asset_empty_raises(tmp_path):
    with pytest.raises(cam.ScanError):
        cam.scan_asset(str(tmp_path), [])


# ---------------------------------------------------------------------------
# 3. Filename leakage + redaction (HIGH-8)
# ---------------------------------------------------------------------------

def test_path_hit_index_detects_leaky_name(matcher):
    assert matcher.path_hit_index(f'dir/{FAKE}_notes.txt') is not None
    assert matcher.path_hit_index('dir/clean_notes.txt') is None


def test_redact_path_opaque_and_no_echo(matcher):
    leaky = f'dir/{FAKE}_notes.txt'
    redacted = matcher.redact_path(leaky)
    assert redacted.startswith('<redacted-path:')
    for tok in ALL_FAKES:
        assert tok not in redacted
    assert matcher.redact_path('dir/clean.txt') == 'dir/clean.txt'


# ---------------------------------------------------------------------------
# 4. REAL Brotli payloads (HIGH-4)
# ---------------------------------------------------------------------------

def test_brotli_decompressed_scanned(tmp_path, matcher):
    brotli = pytest.importorskip('brotli')
    payload = f"binary-header{FAKE}binary-trailer".encode('utf-8')
    f = tmp_path / 'atlas-v1.bin.br'
    f.write_bytes(brotli.compress(payload))
    issues = cam.scan_asset(str(f), [FAKE, FAKE_HE, FAKE_DOMAIN])
    assert any('brotli-decompressed' in i.path for i in issues), (
        "leak hidden inside a Brotli payload was not caught after decompression"
    )
    _assert_never_echoes(issues)


def test_brotli_corrupt_fails_closed(tmp_path):
    pytest.importorskip('brotli')
    f = tmp_path / 'broken.bin.br'
    f.write_bytes(b'\x00\x01\x02 not valid brotli \xff\xfe')
    with pytest.raises(cam.ScanError):
        cam.scan_asset(str(f), [FAKE])


def test_committed_golden_brotli_fixture_is_clean():
    """The tracked golden fixture must decompress and scan clean under the real
    pattern set (only runs when the pattern env is configured, e.g. locally/CI)."""
    if not os.environ.get('MASKING_SCAN_PATTERNS_FILE'):
        pytest.skip("MASKING_SCAN_PATTERNS_FILE not set")
    pytest.importorskip('brotli')
    fixture = REPO_ROOT / 'tests' / 'fixtures' / 'atlas' / 'golden-v1.bin.br'
    if not fixture.is_file():
        pytest.skip("golden fixture missing")
    patterns = cam.load_patterns()
    issues = cam.scan_asset(str(fixture), patterns)
    assert issues == [], "the committed golden Brotli fixture must scan clean"


# ---------------------------------------------------------------------------
# 5. The ACTUAL scan_repo path against REAL temp git repos
# ---------------------------------------------------------------------------

def test_repo_clean(git_repo):
    (git_repo / 'a.txt').write_text('nothing here\n', encoding='utf-8')
    _git(git_repo, 'add', 'a.txt')
    _git(git_repo, 'commit', '-q', '-m', 'clean')
    assert cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN]) == []


def test_repo_head_blob(git_repo):
    """Leak committed to HEAD, then scrubbed in BOTH index and worktree -- only
    the HEAD blob still carries it. Proves HEAD blobs are scanned."""
    f = git_repo / 'doc.txt'
    f.write_text(f'leak {FAKE}\n', encoding='utf-8')
    _git(git_repo, 'add', 'doc.txt')
    _git(git_repo, 'commit', '-q', '-m', 'oops')
    f.write_text('scrubbed\n', encoding='utf-8')
    _git(git_repo, 'add', 'doc.txt')  # index scrubbed; HEAD still has it
    issues = cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN])
    assert any(i.path.startswith('HEAD:') for i in issues), issues
    _assert_never_echoes(issues)


def test_repo_index_divergence(git_repo):
    """Leak STAGED in the index but scrubbed in the worktree -- proves the
    staged blob is scanned separately from the worktree."""
    (git_repo / 'base.txt').write_text('base\n', encoding='utf-8')
    _git(git_repo, 'add', 'base.txt')
    _git(git_repo, 'commit', '-q', '-m', 'base')
    f = git_repo / 'staged.txt'
    f.write_text(f'{FAKE} staged\n', encoding='utf-8')
    _git(git_repo, 'add', 'staged.txt')   # leak in index
    f.write_text('clean worktree\n', encoding='utf-8')  # worktree scrubbed
    issues = cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN])
    assert any(i.path.startswith('INDEX:') for i in issues), issues


def test_repo_worktree_modification(git_repo):
    """Leak added to a tracked file's WORKTREE copy but not staged."""
    f = git_repo / 'w.txt'
    f.write_text('clean\n', encoding='utf-8')
    _git(git_repo, 'add', 'w.txt')
    _git(git_repo, 'commit', '-q', '-m', 'clean')
    f.write_text(f'now with {FAKE}\n', encoding='utf-8')  # unstaged edit
    issues = cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN])
    assert any(i.path == 'w.txt' for i in issues), issues


def test_repo_untracked(git_repo):
    (git_repo / 'base.txt').write_text('base\n', encoding='utf-8')
    _git(git_repo, 'add', 'base.txt')
    _git(git_repo, 'commit', '-q', '-m', 'base')
    (git_repo / 'new.txt').write_text(f'{FAKE}\n', encoding='utf-8')  # untracked
    issues = cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN])
    assert any(i.path.startswith('UNTRACKED:') for i in issues), issues


def test_repo_gitignored_excluded(git_repo):
    (git_repo / '.gitignore').write_text('secret/\n', encoding='utf-8')
    _git(git_repo, 'add', '.gitignore')
    _git(git_repo, 'commit', '-q', '-m', 'ignore')
    (git_repo / 'secret').mkdir()
    (git_repo / 'secret' / 'leak.txt').write_text(f'{FAKE}\n', encoding='utf-8')
    assert cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN]) == []


def test_repo_encoded_leak_in_committed(git_repo):
    """A JS-escaped leak committed into a source file is caught."""
    escaped = '"' + ''.join(f'\\u{ord(c):04x}' for c in FAKE) + '"'
    (git_repo / 'app.js').write_text(f'var x = {escaped};\n', encoding='utf-8')
    _git(git_repo, 'add', 'app.js')
    _git(git_repo, 'commit', '-q', '-m', 'encoded')
    issues = cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN])
    assert issues, "JS-escaped leak in a committed blob was not caught"


def test_repo_filename_leak_redacted(git_repo):
    """A committed file whose NAME contains the token is flagged and the path is
    redacted in the reported Issue (HIGH-8)."""
    leaky = git_repo / f'{FAKE}_data.txt'
    leaky.write_text('body is clean\n', encoding='utf-8')
    _git(git_repo, 'add', leaky.name)
    _git(git_repo, 'commit', '-q', '-m', 'leaky name')
    issues = cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN])
    assert any(i.surface == 'filename' for i in issues), issues
    _assert_never_echoes(issues)
    assert all(i.path.startswith('<redacted-path:') or FAKE not in i.path
               for i in issues)


def test_repo_nonascii_path(git_repo):
    d = git_repo / 'papké'
    d.mkdir()
    (d / 'notes.txt').write_text(f'{FAKE}\n', encoding='utf-8')
    _git(git_repo, 'add', '-A')
    _git(git_repo, 'commit', '-q', '-m', 'nonascii')
    issues = cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN])
    assert issues, "leak under a non-ASCII path was not caught"


# ---------------------------------------------------------------------------
# 6. NUL-delimited parsing robustness incl. embedded-newline paths (HIGH-3)
# ---------------------------------------------------------------------------

def test_ls_tree_parser_handles_embedded_newline_path():
    # git -z terminates records with NUL, so a newline INSIDE a path must not
    # split the record. (Real newline-in-name files can't be created on Windows,
    # so we exercise the parser directly with synthetic -z bytes.)
    path = b'dir/we\nird\tname.txt'  # newline AND a tab inside the name
    rec = b'100644 blob ' + b'a' * 40 + b'\t' + path
    parsed = list(cam._parse_ls_tree_z([rec]))
    assert parsed == [('a' * 40, path)]


def test_ls_files_stage_parser_handles_embedded_newline_path():
    path = b'sub/od\nd.bin'
    rec = b'100644 ' + b'b' * 40 + b' 0\t' + path
    parsed = list(cam._parse_ls_files_stage_z([rec]))
    assert parsed == [('b' * 40, path)]


def test_ls_tree_parser_malformed_fails_closed():
    with pytest.raises(cam.ScanError):
        list(cam._parse_ls_tree_z([b'garbage-without-fields']))


# ---------------------------------------------------------------------------
# 7. Fail-CLOSED policy (HIGH-2)
# ---------------------------------------------------------------------------

def test_git_command_failure_fails_closed():
    with pytest.raises(cam.ScanError):
        cam._git(['definitely-not-a-git-subcommand-xyz'])


def test_scan_repo_in_nongit_dir_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(cam, 'ROOT_DIR', tmp_path)  # not a git repo
    with pytest.raises(cam.ScanError):
        cam.scan_repo([FAKE])


def test_batch_read_missing_sha_fails_closed(git_repo):
    with pytest.raises(cam.ScanError):
        cam._batch_read_shas(['0' * 40])  # well-formed but nonexistent object


def test_unreadable_worktree_file_fails_closed(git_repo, monkeypatch):
    (git_repo / 'base.txt').write_text('base\n', encoding='utf-8')
    _git(git_repo, 'add', 'base.txt')
    _git(git_repo, 'commit', '-q', '-m', 'base')
    (git_repo / 'unreadable.txt').write_text('present but will fail to read\n',
                                             encoding='utf-8')

    real_read = Path.read_bytes

    def boom(self, *a, **k):
        if self.name == 'unreadable.txt':
            raise OSError("simulated unreadable file")
        return real_read(self, *a, **k)

    monkeypatch.setattr(Path, 'read_bytes', boom)
    with pytest.raises(cam.ScanError):
        cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN])


def test_bom_decode_failure_fails_closed(matcher):
    # UTF-16 BOM followed by an odd trailing byte -> strict decode raises.
    bad = b'\xff\xfe' + b'A\x00B'  # odd length after BOM
    with pytest.raises(cam.ScanError):
        matcher.scan(bad, 'fx')


# ---------------------------------------------------------------------------
# 8. scan_asset directory forms + --strict CI mode (HIGH-7)
# ---------------------------------------------------------------------------

def test_scan_asset_single_file(tmp_path, matcher):
    f = tmp_path / 'atlas-v1.bin'
    f.write_bytes(f"header{FAKE}trailer".encode('utf-8'))
    assert cam.scan_asset(str(f), [FAKE, FAKE_HE, FAKE_DOMAIN])


def test_scan_asset_directory_recursive(tmp_path):
    (tmp_path / 'sub').mkdir()
    (tmp_path / 'manifest.json').write_text('{"ok": true}', encoding='utf-8')
    (tmp_path / 'sub' / 'atlas-v1.bin').write_bytes(
        f"blob-{FAKE}-inside".encode('utf-8'))
    issues = cam.scan_asset(str(tmp_path), [FAKE, FAKE_HE, FAKE_DOMAIN])
    assert any('atlas-v1.bin' in i.path for i in issues), issues


def test_scan_asset_nonstrict_skips_irrelevant_ext(tmp_path):
    (tmp_path / 'notes.rst').write_text(FAKE, encoding='utf-8')
    issues = cam.scan_asset(str(tmp_path), [FAKE, FAKE_HE, FAKE_DOMAIN])
    assert not any('notes.rst' in i.path for i in issues)


def test_scan_asset_strict_scans_every_file(tmp_path):
    """--strict scans EVERY regular file, including a suffix a non-strict walk
    would skip (.rst here)."""
    (tmp_path / 'notes.rst').write_text(FAKE, encoding='utf-8')
    issues = cam.scan_asset(str(tmp_path), [FAKE, FAKE_HE, FAKE_DOMAIN], strict=True)
    assert any('notes.rst' in i.path for i in issues), issues


def test_scan_asset_strict_empty_dir_fails_closed(tmp_path):
    d = tmp_path / 'empty'
    d.mkdir()
    with pytest.raises(cam.ScanError):
        cam.scan_asset(str(d), [FAKE], strict=True)


def test_scan_asset_strict_missing_path_fails_closed(tmp_path):
    with pytest.raises(cam.ScanError):
        cam.scan_asset(str(tmp_path / 'nope'), [FAKE], strict=True)


def test_scan_asset_nonexistent_nonstrict_returns_empty(tmp_path):
    assert cam.scan_asset(str(tmp_path / 'nope'), [FAKE]) == []


# ---------------------------------------------------------------------------
# 9. CLI end-to-end + fail-safe env
# ---------------------------------------------------------------------------

def test_load_patterns_empty_when_env_unset(monkeypatch):
    monkeypatch.delenv('MASKING_SCAN_PATTERNS_FILE', raising=False)
    assert cam.load_patterns() == []


def test_load_patterns_reads_and_filters(tmp_path, monkeypatch):
    p = _write_patterns_file(tmp_path, [FAKE, '# comment', '', 'second'])
    monkeypatch.setenv('MASKING_SCAN_PATTERNS_FILE', str(p))
    assert cam.load_patterns() == [FAKE, 'second']


def test_cli_exits_nonzero_when_patterns_unset():
    result = _run_cli({}, ['--scan-asset', str(REPO_ROOT / 'scripts')])
    assert result.returncode == 1
    assert 'ERROR' in (result.stdout + result.stderr)


def test_cli_self_test_passes():
    result = _run_cli({}, ['--self-test'])
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'PASS' in result.stdout


def test_cli_self_test_rejects_mixed_with_scan():
    result = _run_cli({}, ['--self-test', '--scan-repo'])
    assert result.returncode == 2
    assert 'ERROR' in (result.stdout + result.stderr)


def test_cli_strict_requires_both_surfaces(tmp_path):
    pf = _write_patterns_file(tmp_path, [FAKE])
    result = _run_cli({'MASKING_SCAN_PATTERNS_FILE': str(pf)},
                      ['--strict', '--scan-repo'])
    assert result.returncode == 2
    assert 'ERROR' in (result.stdout + result.stderr)


def test_cli_scan_asset_catches_and_never_echoes(tmp_path):
    pf = _write_patterns_file(tmp_path, [FAKE])
    asset = tmp_path / 'asset'
    asset.mkdir()
    (asset / 'manifest.json').write_text(f'{{"n": "{FAKE}"}}', encoding='utf-8')
    result = _run_cli({'MASKING_SCAN_PATTERNS_FILE': str(pf)},
                      ['--scan-asset', str(asset)])
    assert result.returncode == 1, result.stdout + result.stderr
    assert FAKE not in result.stdout and FAKE not in result.stderr


def test_cli_scan_asset_clean_exits_zero(tmp_path):
    pf = _write_patterns_file(tmp_path, [FAKE])
    asset = tmp_path / 'asset'
    asset.mkdir()
    (asset / 'manifest.json').write_text('{"n": "nothing"}', encoding='utf-8')
    result = _run_cli({'MASKING_SCAN_PATTERNS_FILE': str(pf)},
                      ['--scan-asset', str(asset)])
    assert result.returncode == 0, result.stdout + result.stderr


if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v']))
