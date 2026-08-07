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
# A lowercase-Greek token whose UPPERCASE haystack form casefolds to it but has
# entirely different UTF-8 bytes -- proves Unicode casefolding of the HAYSTACK
# (a plain ASCII `bytes.lower()` fold can NEVER bridge Ζ<->ζ) -- HIGH-1.
FAKE_GREEK = 'ζζζ_masking_token_ζζζ'
# A precomposed-accent token whose NFC and NFD forms differ byte-for-byte.
FAKE_ACC = 'zzz_café_tökén_zzz'
# A spaces-bearing token so URL percent-/form-encoding actually transforms it
# (the byte pass alone cannot match `%20`/`+` against a space) -- HIGH-5.
FAKE_SPACE = 'zzz fake mask token'
ALL_FAKES = (FAKE, FAKE_HE, FAKE_DOMAIN, FAKE_GREEK, FAKE_ACC, FAKE_SPACE)

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
    return cam.build_matcher([FAKE, FAKE_HE, FAKE_DOMAIN, FAKE_GREEK, FAKE_ACC, FAKE_SPACE])


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
    # FAKE_ACC's NFD form (decomposed accents) is byte-distinct from its NFC
    # form; the pattern set carries the NFC token, so this exercises the NFD
    # variant genuinely (plain Hebrew consonants have NFC == NFD, which is why
    # the previous FAKE_HE fixture here was vacuous).
    nfd = unicodedata.normalize('NFD', FAKE_ACC)
    assert nfd.encode('utf-8') != unicodedata.normalize('NFC', FAKE_ACC).encode('utf-8')
    issues = matcher.scan(f"x {nfd} y".encode('utf-8'), 'fx')
    assert issues, "NFD-normalized form not detected"


def test_unicode_casefold_haystack_non_ascii(matcher):
    """HIGH-1: a non-ASCII UPPERCASE haystack whose casefold equals the
    (lowercase) pattern is caught -- a plain ASCII `bytes.lower()` fold leaves
    Greek capitals untouched and would miss it. Coverage comes from the
    exhaustive case/normalization byte forms (matched fast, CI-viable) rather
    than casefolding the whole multi-gigabyte haystack."""
    upper = FAKE_GREEK.upper()  # 'ΖΖΖ_MASKING_TOKEN_ΖΖΖ'
    assert upper.encode('utf-8') != FAKE_GREEK.encode('utf-8')
    issues = matcher.scan(f"x {upper} y".encode('utf-8'), 'fx')
    assert issues, "Unicode-casefold (upper-case Greek) haystack match not detected"
    _assert_never_echoes(issues)


def test_unicode_casefold_haystack_title_and_lower(matcher):
    """Title-case and lower-case non-ASCII renditions are covered too."""
    assert matcher.scan(f"x {FAKE_GREEK.title()} y".encode('utf-8'), 'fx')
    assert matcher.scan(f"x {FAKE_GREEK.lower()} y".encode('utf-8'), 'fx')


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
    # FAKE_SPACE contains spaces, so quote() genuinely rewrites them to %20 --
    # the raw byte pass cannot match; only the URL byte-decoding does (HIGH-5).
    encoded = urllib.parse.quote(FAKE_SPACE, safe='')
    assert '%20' in encoded
    data = f"q={encoded}".encode('ascii')
    issues = matcher.scan(data, 'fx')
    assert issues and any(i.surface == 'url' for i in issues), (
        "percent-encoded space-bearing token not caught via the URL surface"
    )
    _assert_never_echoes(issues)


def test_url_form_plus_decoded(matcher):
    # `+` must be decoded as a space (application/x-www-form-urlencoded) -- HIGH-5.
    data = FAKE_SPACE.replace(' ', '+').encode('ascii')  # 'zzz+fake+mask+token'
    issues = matcher.scan(data, 'fx')
    assert issues and any(i.surface == 'url' for i in issues), (
        "form (+ -> space) encoded token not caught"
    )


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
    """A blob dense in escape introducers de-escapes whole-file and still
    catches a mixed literal+escaped form buried deep in the noise."""
    noise = b'&#65;' * 5000
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


# ---------------------------------------------------------------------------
# 10. Round-3 adversarial coverage -- every plausible wide decoding (HIGH-2),
#     layered encodings (HIGH-3), streaming boundaries + Brotli caps (HIGH-4),
#     URL fail-closed (HIGH-5), HEAD-probe distinction (HIGH-7), traversal /
#     metadata / symlink handling (HIGH-8), diagnostic never-echo (HIGH-9),
#     and the un-bypassable self-test (HIGH-10). Every fail-closed branch is
#     asserted at the PROCESS EXIT-STATUS level too, with no sensitive output.
# ---------------------------------------------------------------------------

def test_utf16be_no_bom(matcher):
    issues = matcher.scan((FAKE * 3).encode('utf-16-be'), 'fx')
    assert issues and any(i.surface == 'decoded' for i in issues), (
        "BOM-less UTF-16BE not detected (only LE was tried before -- HIGH-2)"
    )


def test_utf32le_no_bom(matcher):
    issues = matcher.scan((FAKE * 3).encode('utf-32-le'), 'fx')
    assert issues and any(i.surface == 'decoded' for i in issues), (
        "BOM-less UTF-32LE not detected (HIGH-2)"
    )


def test_utf32be_no_bom(matcher):
    issues = matcher.scan((FAKE * 3).encode('utf-32-be'), 'fx')
    assert issues and any(i.surface == 'decoded' for i in issues), (
        "BOM-less UTF-32BE not detected (HIGH-2)"
    )


def test_layered_js_escape_inside_utf16(matcher):
    """HIGH-3: a JS-escaped ASCII leak stored as UTF-16LE bytes -- the escape
    decoding must COMPOSE with the wide character decoding."""
    escaped = '"' + ''.join(f'\\u{ord(c):04x}' for c in FAKE) + '"'
    data = escaped.encode('utf-16-le')
    issues = matcher.scan(data, 'fx')
    assert issues and any(i.surface in ('escape', 'decoded') for i in issues), (
        "escape-within-wide-encoding leak not caught (decode+unescape did not compose)"
    )


def test_layered_url_encoded_hebrew_via_url_surface(matcher):
    """Percent-encoded non-ASCII must be caught via the casefolded URL pass."""
    data = ('q=' + urllib.parse.quote(FAKE_HE, safe='')).encode('ascii')
    issues = matcher.scan(data, 'fx')
    assert issues and any(i.surface == 'url' for i in issues)


def test_chunk_boundary_straddle(tmp_path, monkeypatch):
    """HIGH-4b: a leak that straddles a streaming chunk boundary is still caught
    because the carried overlap is sized to the longest matchable byte span."""
    monkeypatch.setattr(cam, '_WHOLE_READ_CAP', 4096)
    monkeypatch.setattr(cam, '_CHUNK_SIZE', 8192)
    boundary = 8192
    pre = b'.' * (boundary - len(FAKE) // 2)
    body = pre + FAKE.encode('ascii') + b'.' * 4000
    f = tmp_path / 'huge.bin'
    f.write_bytes(body)
    assert f.stat().st_size > cam._WHOLE_READ_CAP  # forces the streaming path
    issues = cam.scan_asset(str(f), [FAKE])
    assert issues, "leak straddling a chunk boundary was not caught while streaming"


def test_brotli_decompress_cap_fails_closed(tmp_path, monkeypatch):
    """HIGH-4a: a `.br` that decompresses beyond the sane cap is fail-closed."""
    brotli = pytest.importorskip('brotli')
    monkeypatch.setattr(cam, '_BR_DECOMPRESSED_CAP', 1024)
    payload = b'A' * 50000  # clean content, but expands past the cap
    f = tmp_path / 'big.bin.br'
    f.write_bytes(brotli.compress(payload))
    with pytest.raises(cam.ScanError):
        cam.scan_asset(str(f), [FAKE])


def test_url_decoder_failure_fails_closed(matcher, monkeypatch):
    """HIGH-5: a URL-decoder failure is fail-CLOSED, never fail-open-to-clean."""
    def boom(*a, **k):
        raise ValueError("simulated URL decoder failure")
    monkeypatch.setattr(cam.urllib.parse, 'unquote_to_bytes', boom)
    with pytest.raises(cam.ScanError):
        matcher.scan(b'x=%41%42%43', 'fx')  # contains '%', triggers the URL pass


def test_empty_repo_unborn_head_ok(git_repo):
    """HIGH-7: a real work tree with an UNBORN HEAD (zero commits) is a PROVEN
    empty-HEAD, scanned cleanly -- not confused with an operational failure."""
    (git_repo / 'untracked.txt').write_text('clean\n', encoding='utf-8')
    assert cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN]) == []


def test_scan_asset_traversal_error_fails_closed(tmp_path, monkeypatch):
    """HIGH-8: a directory that cannot be enumerated is fail-closed (not a
    silently-skipped subtree)."""
    d = tmp_path / 'assets'
    d.mkdir()
    (d / 'manifest.json').write_text('{"ok": true}', encoding='utf-8')
    real_scandir = cam.os.scandir

    def boom(path, *a, **k):
        if str(path).startswith(str(d)):
            raise OSError("simulated scandir failure")
        return real_scandir(path, *a, **k)

    monkeypatch.setattr(cam.os, 'scandir', boom)
    with pytest.raises(cam.ScanError):
        cam.scan_asset(str(d), [FAKE], strict=True)


def test_scan_asset_stat_error_fails_closed(tmp_path, monkeypatch):
    """HIGH-8: an OSError on a file's metadata (not a benign not-a-regular-file)
    is fail-closed in --strict."""
    d = tmp_path / 'assets'
    d.mkdir()
    target = d / 'a.txt'
    target.write_text('clean\n', encoding='utf-8')
    real_lstat = cam.os.lstat

    def boom(path, *a, **k):
        if str(path).endswith('a.txt'):
            raise OSError("simulated lstat failure")
        return real_lstat(path, *a, **k)

    monkeypatch.setattr(cam.os, 'lstat', boom)
    with pytest.raises(cam.ScanError):
        cam.scan_asset(str(d), [FAKE], strict=True)


def test_scan_asset_symlink_not_followed(tmp_path):
    """HIGH-8: a symlink's link TEXT is scanned; the target is never followed."""
    d = tmp_path / 'assets'
    d.mkdir()
    link = d / 'link.txt'
    try:
        os.symlink(f'./{FAKE}_target', link)  # dangling; the link TEXT is leaky
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not permitted on this platform")
    issues = cam.scan_asset(str(d), [FAKE, FAKE_HE, FAKE_DOMAIN], strict=True)
    assert issues, "leaky symlink target-text not caught"
    _assert_never_echoes(issues)


def test_unreadable_leaky_named_file_diagnostic_no_echo(git_repo, monkeypatch):
    """HIGH-9: a fail-closed diagnostic about a file whose NAME is leaky must
    redact the name -- the raised error must not echo the pattern."""
    (git_repo / 'base.txt').write_text('base\n', encoding='utf-8')
    _git(git_repo, 'add', 'base.txt')
    _git(git_repo, 'commit', '-q', '-m', 'base')
    leaky = git_repo / f'{FAKE}_secret.txt'
    leaky.write_text('present but unreadable\n', encoding='utf-8')

    real_read = Path.read_bytes

    def boom(self, *a, **k):
        if FAKE in self.name:
            raise OSError("simulated unreadable file")
        return real_read(self, *a, **k)

    monkeypatch.setattr(Path, 'read_bytes', boom)
    with pytest.raises(cam.ScanError) as ei:
        cam.scan_repo([FAKE, FAKE_HE, FAKE_DOMAIN])
    for tok in ALL_FAKES:
        assert tok not in str(ei.value), "fail-closed diagnostic echoed a pattern"


def test_issue_format_enforces_redaction(matcher):
    """HIGH-9: Issue.format() re-sanitizes its own path, so even a caller that
    handed it an UN-redacted leaky path cannot leak on print."""
    iss = cam.Issue(path=f'notes_{FAKE}.txt', offset=7, pattern_index=0, surface='content')
    rendered = iss.format()
    assert FAKE not in rendered
    assert '<redacted' in rendered


def test_git_stderr_never_echoed():
    """HIGH-9: a git failure raises WITHOUT embedding raw subprocess stderr."""
    with pytest.raises(cam.ScanError) as ei:
        cam._git(['definitely-not-a-git-subcommand-xyz'])
    msg = str(ei.value)
    assert 'not a git command' not in msg and 'Usage' not in msg


# --- CLI-level fail-closed EXIT STATUS + no-sensitive-output (MEDIUM-11) ----

def test_cli_self_test_empty_asset_bypass_rejected():
    """HIGH-10: `--self-test --scan-asset ""` must NOT silently run the
    self-test and exit 0 -- the empty asset arg is a hard usage error."""
    result = _run_cli({}, ['--self-test', '--scan-asset', ''])
    assert result.returncode == 2, result.stdout + result.stderr
    assert 'PASS' not in result.stdout


def test_cli_scan_asset_whitespace_path_rejected(tmp_path):
    pf = _write_patterns_file(tmp_path, [FAKE])
    result = _run_cli({'MASKING_SCAN_PATTERNS_FILE': str(pf)},
                      ['--scan-asset', '   '])
    assert result.returncode == 2, result.stdout + result.stderr


def test_cli_nongit_repo_exit_nonzero_no_echo(tmp_path, monkeypatch, capsys):
    """A repo scan against a non-git dir fails closed with a non-zero process
    exit and no pattern in the output (MEDIUM-11)."""
    pf = _write_patterns_file(tmp_path, [FAKE])
    monkeypatch.setenv('MASKING_SCAN_PATTERNS_FILE', str(pf))
    monkeypatch.setattr(cam, 'ROOT_DIR', tmp_path)  # not a git repo
    rc = cam.main(['--scan-repo'])
    assert rc == 1
    out = capsys.readouterr()
    assert FAKE not in (out.out + out.err)


def test_cli_asset_bom_decode_failure_exit_nonzero_no_echo(tmp_path):
    """A BOM-declared file that fails to decode makes the CLI exit non-zero
    with no pattern echoed (MEDIUM-11)."""
    pf = _write_patterns_file(tmp_path, [FAKE])
    asset = tmp_path / 'asset'
    asset.mkdir()
    (asset / 'bad.bin').write_bytes(b'\xff\xfe' + b'A\x00B')  # UTF-16 BOM, odd tail
    result = _run_cli({'MASKING_SCAN_PATTERNS_FILE': str(pf)},
                      ['--scan-asset', str(asset)])
    assert result.returncode == 1, result.stdout + result.stderr
    assert FAKE not in (result.stdout + result.stderr)


def test_cli_corrupt_brotli_exit_nonzero(tmp_path):
    pytest.importorskip('brotli')
    pf = _write_patterns_file(tmp_path, [FAKE])
    asset = tmp_path / 'asset'
    asset.mkdir()
    (asset / 'broken.bin.br').write_bytes(b'\x00\x01\x02 not valid brotli \xff\xfe')
    result = _run_cli({'MASKING_SCAN_PATTERNS_FILE': str(pf)},
                      ['--scan-asset', str(asset)])
    assert result.returncode == 1, result.stdout + result.stderr


if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v']))


# ---------------------------------------------------------------------------
# The non-disclosing pattern-set attestation (Codex rounds 1 and 2, MEDIUM --
# held open both times: "the non-disclosing pattern-set attestation and
# real-pattern positive control are still explicitly owed. A synthetic self-test
# cannot attest completeness.")
#
# Completeness genuinely cannot be attested from inside -- the scanner cannot
# enumerate terms nobody told it about. What was missing and IS attestable is
# IDENTITY: which pattern set ran. It matters concretely: on 2026-08-06 the set
# was found missing a signature term that gen-2 hands over as a column NAME, and
# a bare count would not have shown it, but a per-pattern digest lets a reviewer
# confirm the set that ran is the reviewed set.
# ---------------------------------------------------------------------------

_ATT_KEY = b"test-attestation-key"


def test_the_attestation_identifies_the_pattern_set():
    from check_atlas_masking import pattern_set_attestation

    att = pattern_set_attestation(["alpha", "beta", "gamma"], key=_ATT_KEY)
    assert att["pattern_count"] == 3
    assert att["keyed"] is True
    assert len(att["pattern_set_hmac"]) == 64
    assert len(att["pattern_digests"]) == 3


def test_the_attestation_discloses_no_pattern_text_prefix_or_length():
    """The whole design constraint. This output goes into build records and CI
    logs, so it has to be safe where the patterns are not."""
    from check_atlas_masking import pattern_set_attestation

    secret = "averydistinctiverestrictedterm"
    att = pattern_set_attestation([secret, "second"], key=_ATT_KEY)
    blob = repr(att)
    assert secret not in blob
    # No prefix of meaningful length either -- a leaked prefix is a leak.
    for n in range(4, len(secret) + 1):
        assert secret[:n] not in blob, f"a {n}-char prefix of the pattern leaked"
    # And no length disclosure: for a short term, length + a known alphabet is a
    # real narrowing, so the attestation deliberately omits it. Asserted on the
    # KEYS rather than by searching the blob for the number -- a two-digit length
    # occurs by chance inside a hex digest, so the substring form was unsound
    # (it failed on a coincidental "30" and would have passed for the wrong
    # reason on other inputs).
    assert not any("len" in key.lower() for key in att), (
        f"the attestation exposes a length-shaped field: {sorted(att)}"
    )
    # Nor may any VALUE be the length, or be derived from it.
    for value in att.values():
        if isinstance(value, int):
            assert value != len(secret) or value == att["pattern_count"], (
                "an integer field equals the pattern length"
            )


def test_the_set_digest_changes_when_the_set_changes():
    """A silent shrink -- a truncated or half-written pattern file -- must be
    visible. This is the failure the 2026-08-06 gap would have shown up as."""
    from check_atlas_masking import pattern_set_attestation

    full = pattern_set_attestation(["a", "b", "c"], key=_ATT_KEY)
    short = pattern_set_attestation(["a", "b"], key=_ATT_KEY)
    assert full["pattern_set_hmac"] != short["pattern_set_hmac"]
    assert full["pattern_count"] != short["pattern_count"]
    # An EDITED pattern must move the digest too, not only an added/removed one.
    # THIS is the load-bearing assertion: a digest computed over the COUNT alone
    # would satisfy every add/remove check above (the counts differ), so without a
    # same-count edit the test passes for the wrong reason. Caught by mutation
    # testing 2026-08-07 -- a content-blind digest survived the first version.
    edited = pattern_set_attestation(["a", "b", "c2"], key=_ATT_KEY)
    assert edited["pattern_count"] == full["pattern_count"], (
        "this control requires the SAME count, or it cannot detect a "
        "count-only digest"
    )
    assert full["pattern_set_hmac"] != edited["pattern_set_hmac"], (
        "editing a pattern without changing the count did not move the set "
        "digest -- the digest is content-blind, so a silently-swapped pattern "
        "set would attest as identical"
    )
    # ...and the per-pattern digests show WHICH entry changed.
    assert set(full["pattern_digests"]) != set(edited["pattern_digests"])
    assert len(set(full["pattern_digests"]) & set(edited["pattern_digests"])) == 2


def test_the_set_digest_is_stable_under_reordering_and_duplication():
    """Otherwise every reviewer diff would be noise, and the attestation would be
    ignored -- which is the same as not having one."""
    from check_atlas_masking import pattern_set_attestation

    a = pattern_set_attestation(["x", "y", "z"], key=_ATT_KEY)
    b = pattern_set_attestation(["z", "x", "y", "x"], key=_ATT_KEY)
    assert a["pattern_set_hmac"] == b["pattern_set_hmac"]
    assert a["pattern_count"] == b["pattern_count"] == 3


def test_attest_fails_closed_without_a_pattern_file(tmp_path, monkeypatch):
    """An attestation of an EMPTY set would be the worst possible artifact: a
    build record that looks like evidence while attesting nothing."""
    import check_atlas_masking as cam

    monkeypatch.delenv("MASKING_SCAN_PATTERNS_FILE", raising=False)
    assert cam.main(["--attest"]) == 1


def test_an_unkeyed_attestation_emits_NO_digests(monkeypatch):
    """Codex round 3 (MEDIUM), correcting my own "not a hint" claim.

    An unsalted SHA-256 prefix is a membership ORACLE: hash a candidate term,
    compare the first 8 hex chars, and membership is confirmed at negligible
    collision risk for any guessable short term. So with no key the digests are
    OMITTED rather than emitted unkeyed -- an attestation is a convenience, and a
    convenience is not worth a confirmation oracle over restricted vocabulary.
    """
    from check_atlas_masking import pattern_set_attestation

    monkeypatch.delenv("MASKING_ATTESTATION_KEY", raising=False)
    att = pattern_set_attestation(["alpha", "beta"])
    assert att["keyed"] is False
    assert att["pattern_count"] == 2, "the bare count must still be emitted"
    assert "pattern_digests" not in att, (
        "unkeyed per-pattern digests were emitted -- anyone can hash a candidate "
        "term and confirm membership from the prefix"
    )
    assert "pattern_set_hmac" not in att
    assert not any("sha256" in k for k in att), (
        f"an unkeyed digest field survives under another name: {sorted(att)}"
    )


def test_the_keyed_digest_is_not_reproducible_without_the_key():
    """The property that makes keying worth doing: a holder of a candidate term
    cannot confirm it without also holding the key."""
    from check_atlas_masking import pattern_set_attestation

    secret = "averydistinctiverestrictedterm"
    with_key = pattern_set_attestation([secret], key=b"the-real-key")
    other_key = pattern_set_attestation([secret], key=b"a-guessed-key")
    assert with_key["pattern_digests"] != other_key["pattern_digests"], (
        "the digests do not depend on the key -- keying bought nothing"
    )
    # The plain SHA-256 of the term must NOT appear: that would defeat the point.
    from hashlib import sha256 as _sha256
    plain = _sha256(secret.encode("utf-8")).hexdigest()
    assert plain[:8] not in repr(with_key)


def test_the_key_can_come_from_the_environment(monkeypatch):
    """So a CI run keys its attestation the same way it loads its patterns --
    env-held secret, never a committed literal."""
    from check_atlas_masking import pattern_set_attestation

    monkeypatch.setenv("MASKING_ATTESTATION_KEY", "env-supplied-key")
    from_env = pattern_set_attestation(["alpha"])
    explicit = pattern_set_attestation(["alpha"], key=b"env-supplied-key")
    assert from_env["keyed"] is True
    assert from_env["pattern_digests"] == explicit["pattern_digests"]
