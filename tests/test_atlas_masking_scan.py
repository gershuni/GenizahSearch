# -*- coding: utf-8 -*-
"""Sanity-injection self-test for scripts/check_atlas_masking.py (D-07).

Mirrors tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation:
inject a FABRICATED test-only known-bad token (NEVER the real restricted
M-source string) and assert the scan flags it. Covers a worktree-file hit, a
UTF-8-bytes-in-a-binary hit, and an encoded/escaped-form hit; asserts clean
content passes; asserts the fail-safe (no patterns loaded -> exit 1) and
never-echo (caught-fixture output never contains the fabricated pattern)
guarantees hold.
"""
import os
import subprocess
import sys
import urllib.parse
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / 'scripts' / 'check_atlas_masking.py'

sys.path.insert(0, str(REPO_ROOT / 'scripts'))
import check_atlas_masking as cam  # noqa: E402

# A fabricated, test-only token -- NEVER the real restricted M-source string.
FAKE_PATTERN = 'ZZZ_FAKE_MASKING_TOKEN_ZZZ'


def _write_patterns_file(tmp_path: Path, patterns) -> Path:
    p = tmp_path / 'patterns.txt'
    p.write_text('\n'.join(patterns) + '\n', encoding='utf-8')
    return p


def _run_cli(env_overrides: dict, args: list) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.pop('MASKING_SCAN_PATTERNS_FILE', None)
    env.update(env_overrides)
    return subprocess.run(
        [sys.executable, str(SCRIPT)] + args,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        env=env,
    )


# ---------------------------------------------------------------------------
# Unit-level: the shared matcher (fast, no subprocess)
# ---------------------------------------------------------------------------

def test_literal_utf8_match_detected():
    """A worktree-file-style hit: the pattern appears as plain UTF-8 text."""
    data = f"some content\n{FAKE_PATTERN}\nmore content\n".encode('utf-8')
    issues = cam._scan_bytes('fixture.txt', data, [FAKE_PATTERN])
    assert issues, "literal UTF-8 text match was not detected"
    assert issues[0].pattern_index == 0
    assert issues[0].path == 'fixture.txt'


def test_utf8_bytes_in_binary_detected():
    """A UTF-8-bytes-in-a-binary hit: pattern bytes embedded in otherwise
    non-text binary content (simulates a hit inside a built .bin asset)."""
    binary_noise = bytes(range(0, 256)) * 4
    data = binary_noise[:100] + FAKE_PATTERN.encode('utf-8') + binary_noise[100:]
    issues = cam._scan_bytes('fixture.bin', data, [FAKE_PATTERN])
    assert issues, "UTF-8 bytes embedded in binary content were not detected"


def test_normalized_nfd_form_detected():
    """A composed vs. decomposed Unicode form still matches (NFC/NFD)."""
    import unicodedata
    nfd_pattern = unicodedata.normalize('NFD', FAKE_PATTERN)
    data = f"prefix {nfd_pattern} suffix".encode('utf-8')
    issues = cam._scan_bytes('fixture_nfd.txt', data, [FAKE_PATTERN])
    assert issues, "NFD-normalized form was not detected"


def test_url_encoded_form_detected():
    """An encoded/escaped-form hit: URL percent-encoding."""
    url_enc = urllib.parse.quote(FAKE_PATTERN)
    data = f"https://example.invalid/path?q={url_enc}".encode('ascii')
    issues = cam._scan_bytes('fixture.html', data, [FAKE_PATTERN])
    assert issues, "URL percent-encoded form was not detected"


def test_html_entity_encoded_form_detected():
    """An encoded/escaped-form hit: HTML numeric-entity encoding."""
    html_enc = ''.join(f'&#{ord(c)};' for c in FAKE_PATTERN)
    data = f"<p>{html_enc}</p>".encode('ascii')
    issues = cam._scan_bytes('fixture2.html', data, [FAKE_PATTERN])
    assert issues, "HTML-entity-encoded form was not detected"


def test_js_unicode_escape_form_detected():
    """An encoded/escaped-form hit: JS \\uXXXX escape."""
    js_enc = ''.join(f'\\u{ord(c):04x}' for c in FAKE_PATTERN)
    data = f'var x = "{js_enc}";'.encode('ascii')
    issues = cam._scan_bytes('fixture.js', data, [FAKE_PATTERN])
    assert issues, "JS \\uXXXX-escaped form was not detected"


def test_clean_content_passes():
    """Clean content (no fabricated token anywhere) yields zero issues."""
    data = b"nothing restricted here, just ordinary repo content.\n"
    issues = cam._scan_bytes('clean.txt', data, [FAKE_PATTERN])
    assert issues == []


def test_never_echo_in_reported_issue():
    """The reported Issue never carries the matched pattern text -- only
    path, byte offset, and pattern index."""
    data = f"leak: {FAKE_PATTERN}".encode('utf-8')
    issues = cam._scan_bytes('fixture.txt', data, [FAKE_PATTERN])
    assert issues
    for issue in issues:
        rendered = issue.format()
        assert FAKE_PATTERN not in rendered, (
            "never-echo violated: the fabricated pattern leaked into the "
            "formatted issue string"
        )
        assert FAKE_PATTERN not in repr(issue)


# ---------------------------------------------------------------------------
# scan_asset -- file and directory (recursive) forms
# ---------------------------------------------------------------------------

def test_scan_asset_single_file(tmp_path):
    f = tmp_path / 'atlas-v1.bin'
    f.write_bytes(f"header{FAKE_PATTERN}trailer".encode('utf-8'))
    issues = cam.scan_asset(str(f), [FAKE_PATTERN])
    assert issues, "scan_asset did not catch a hit in a single named file"


def test_scan_asset_directory_recursive(tmp_path):
    (tmp_path / 'sub').mkdir()
    manifest = tmp_path / 'manifest.json'
    manifest.write_text('{"ok": true}', encoding='utf-8')
    leaky = tmp_path / 'sub' / 'atlas-v1.bin.br'
    leaky.write_bytes(f"blob-with-{FAKE_PATTERN}-inside".encode('utf-8'))
    clean_html = tmp_path / 'index.html'
    clean_html.write_text('<html><body>clean</body></html>', encoding='utf-8')

    issues = cam.scan_asset(str(tmp_path), [FAKE_PATTERN])
    hit_paths = {i.path for i in issues}
    assert any('atlas-v1.bin.br' in p for p in hit_paths), (
        f"recursive directory scan did not catch the nested leak; got {hit_paths}"
    )


def test_scan_asset_directory_skips_irrelevant_extensions(tmp_path):
    """A directory walk only inspects asset-relevant suffixes
    (.bin/.bin.br/.json/.html) -- an unrelated file extension inside the same
    directory is not part of the recursive walk's candidate set."""
    irrelevant = tmp_path / 'notes.md'
    irrelevant.write_text(f"{FAKE_PATTERN}", encoding='utf-8')
    issues = cam.scan_asset(str(tmp_path), [FAKE_PATTERN])
    assert not any('notes.md' in i.path for i in issues)


# ---------------------------------------------------------------------------
# Fail-safe: no patterns loaded must NEVER be a silent green
# ---------------------------------------------------------------------------

def test_load_patterns_empty_when_env_unset(monkeypatch):
    monkeypatch.delenv('MASKING_SCAN_PATTERNS_FILE', raising=False)
    assert cam.load_patterns() == []


def test_load_patterns_empty_when_file_missing(monkeypatch, tmp_path):
    monkeypatch.setenv('MASKING_SCAN_PATTERNS_FILE', str(tmp_path / 'does_not_exist.txt'))
    assert cam.load_patterns() == []


def test_load_patterns_empty_when_file_blank(monkeypatch, tmp_path):
    p = tmp_path / 'blank.txt'
    p.write_text('# only a comment\n\n', encoding='utf-8')
    monkeypatch.setenv('MASKING_SCAN_PATTERNS_FILE', str(p))
    assert cam.load_patterns() == []


def test_load_patterns_reads_real_file(tmp_path, monkeypatch):
    p = _write_patterns_file(tmp_path, [FAKE_PATTERN, '# a comment', '', 'second'])
    monkeypatch.setenv('MASKING_SCAN_PATTERNS_FILE', str(p))
    patterns = cam.load_patterns()
    assert patterns == [FAKE_PATTERN, 'second']


def test_cli_exits_nonzero_when_patterns_file_unset():
    result = _run_cli({}, ['--scan-asset', str(REPO_ROOT / 'scripts')])
    assert result.returncode == 1
    assert 'ERROR' in result.stdout or 'ERROR' in result.stderr


def test_cli_exits_nonzero_when_patterns_file_empty(tmp_path):
    p = _write_patterns_file(tmp_path, [])
    # write an empty file explicitly (the helper above always adds a
    # trailing newline to a joined empty list -- force truly empty content)
    p.write_text('', encoding='utf-8')
    result = _run_cli(
        {'MASKING_SCAN_PATTERNS_FILE': str(p)},
        ['--scan-asset', str(REPO_ROOT / 'scripts')],
    )
    assert result.returncode == 1


# ---------------------------------------------------------------------------
# CLI end-to-end: --scan-asset against a real temp fixture
# ---------------------------------------------------------------------------

def test_cli_scan_asset_catches_injected_fixture(tmp_path):
    patterns_file = _write_patterns_file(tmp_path, [FAKE_PATTERN])
    asset_dir = tmp_path / 'asset'
    asset_dir.mkdir()
    (asset_dir / 'manifest.json').write_text(
        f'{{"note": "{FAKE_PATTERN}"}}', encoding='utf-8'
    )

    result = _run_cli(
        {'MASKING_SCAN_PATTERNS_FILE': str(patterns_file)},
        ['--scan-asset', str(asset_dir)],
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert FAKE_PATTERN not in result.stdout, (
        "never-echo violated: CLI output leaked the fabricated pattern"
    )
    assert FAKE_PATTERN not in result.stderr


def test_cli_scan_asset_clean_exits_zero(tmp_path):
    patterns_file = _write_patterns_file(tmp_path, [FAKE_PATTERN])
    asset_dir = tmp_path / 'asset_clean'
    asset_dir.mkdir()
    (asset_dir / 'manifest.json').write_text('{"note": "nothing here"}', encoding='utf-8')

    result = _run_cli(
        {'MASKING_SCAN_PATTERNS_FILE': str(patterns_file)},
        ['--scan-asset', str(asset_dir)],
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_cli_self_test_passes():
    result = _run_cli({}, ['--self-test'])
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'PASS' in result.stdout


if __name__ == '__main__':
    sys.exit(pytest.main([__file__, '-v']))
