#!/usr/bin/env python3
"""
Masking Scan — D-07 (Phase 133 forerunner of the permanent DATA-05 CI guard)

Scans for a restricted reference-corpus name/aliases (referred to everywhere
in committed material only by the internal codename "M-source") across:

  - `scan_repo()` -- three SEPARATE git surfaces: HEAD/index blobs, tracked
    worktree files, and non-ignored untracked candidates. A hit on ANY
    surface is a failure, because a leak can differ between a staged blob
    and the worktree, or sit in an untracked file about to be committed.
    Uses a FAST literal-byte matcher (exact UTF-8 bytes + a cheap ASCII-only
    case-fold via `bytes.lower()`) -- no full Unicode decode/normalize, so
    this stays practical across a real working tree that may carry large
    unrelated non-ignored scratch content (a real CI checkout only ever has
    tracked files anyway; local non-ignored untracked cruft is the one case
    this must stay usable against).
  - `scan_asset(path)` -- a built product asset (a single file OR a whole
    directory, recursively). This is a small, bounded, controlled artifact
    (the baked atlas payload, capped at a few MB), so it gets the RICH
    matcher: every restricted pattern as literal text, as UTF-8 bytes, in
    Unicode-normalized (NFC/NFD + casefold) form, and in common
    encoded/escaped forms (URL percent-encoding, HTML numeric entities, JS
    \\uXXXX escapes) -- a leak baked into a built HTML/JSON/binary asset can
    hide behind any of these.

SECURITY CONTROLS (do not weaken):
  - NEVER hardcode the restricted patterns in this file. `load_patterns()`
    reads them from a gitignored local file referenced by the
    MASKING_SCAN_PATTERNS_FILE env var -- the same env-sourced-secret idiom
    as PUZZLE_UPLOAD_SECRET (web/puzzle_tokens.py).
  - FAIL-SAFE: if the env var is unset, the file is missing, or it yields
    zero patterns, this script prints an error and exits 1. It must NEVER
    exit 0 with an empty pattern set -- a false green here is a HIGH-severity
    data-exposure gap (the masking scan is the sole leak barrier).
  - NEVER ECHO: reported issues include only the relative file path, the
    byte offset, and a pattern INDEX (never the matched pattern text).

Usage:
    python scripts/check_atlas_masking.py --scan-repo
    python scripts/check_atlas_masking.py --scan-asset atlas_data/
    python scripts/check_atlas_masking.py --scan-repo --scan-asset atlas_data/
    python scripts/check_atlas_masking.py --self-test
"""

import argparse
import subprocess
import sys
import unicodedata
import urllib.parse
from dataclasses import dataclass
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent

# File-name suffixes scan_asset walks when given a DIRECTORY (a single file
# argument is always scanned regardless of suffix -- the caller asked for it
# explicitly). Covers baked-atlas outputs: typed/delta binary arrays (+ their
# Brotli-compressed form), the manifest, and any rendered HTML page.
_ASSET_SUFFIXES = ('.bin', '.bin.br', '.br', '.json', '.html', '.htm')


@dataclass(frozen=True)
class Issue:
    """One masking hit. Deliberately carries NO pattern text (never-echo)."""
    path: str
    offset: int
    pattern_index: int

    def format(self) -> str:
        return f"MASK HIT: {self.path} @byte {self.offset} (pattern #{self.pattern_index})"


# ---------------------------------------------------------------------------
# Pattern loading (fail-safe, env-sourced, gitignored file)
# ---------------------------------------------------------------------------

def load_patterns() -> list[str]:
    """Load restricted-string patterns from the gitignored file referenced by
    MASKING_SCAN_PATTERNS_FILE. Returns [] on ANY failure (unset env var,
    missing file, unreadable file, or an all-comment/blank file) -- callers
    MUST treat an empty return as a hard error, never a silent pass."""
    import os
    path_str = os.environ.get('MASKING_SCAN_PATTERNS_FILE')
    if not path_str:
        return []
    p = Path(path_str)
    if not p.is_file():
        return []
    try:
        raw = p.read_text(encoding='utf-8')
    except OSError:
        return []
    patterns = [
        line.strip() for line in raw.splitlines()
        if line.strip() and not line.strip().startswith('#')
    ]
    return patterns


# ---------------------------------------------------------------------------
# Fast literal-byte matcher -- used by scan_repo (the three git surfaces)
# ---------------------------------------------------------------------------

def _scan_bytes_literal(rel_path: str, data: bytes, patterns: list[str]) -> list[Issue]:
    """Fast literal-byte scan: exact UTF-8-byte match (case-sensitive) plus a
    cheap ASCII-only case-insensitive byte match via `bytes.lower()` (which
    only folds A-Z -- Hebrew/other non-ASCII bytes are untouched, so this
    stays a pure byte-level op with NO Unicode decode/normalize). Deliberately
    cheaper than `_scan_bytes` (the rich matcher) so scan_repo's three git
    surfaces stay practical over a real working tree, including large
    non-ignored untracked content that a real CI checkout would never have."""
    issues: list[Issue] = []
    data_lower = None  # computed lazily, at most once per file
    for idx, pattern in enumerate(patterns):
        pat_bytes = pattern.encode('utf-8')
        if not pat_bytes:
            continue
        start = 0
        while True:
            pos = data.find(pat_bytes, start)
            if pos == -1:
                break
            issues.append(Issue(path=rel_path, offset=pos, pattern_index=idx))
            start = pos + 1

        pat_lower = pat_bytes.lower()
        if pat_lower == pat_bytes:
            continue  # pattern has no ASCII letters -- casefold pass is a no-op
        if data_lower is None:
            data_lower = data.lower()
        start = 0
        while True:
            pos = data_lower.find(pat_lower, start)
            if pos == -1:
                break
            issues.append(Issue(path=rel_path, offset=pos, pattern_index=idx))
            start = pos + 1
    return issues


# ---------------------------------------------------------------------------
# Rich multi-form matcher -- used by scan_asset (small, bounded built artifact)
# ---------------------------------------------------------------------------

def _build_search_forms(data: bytes) -> dict:
    """Precompute the expensive, pattern-INDEPENDENT search forms for one
    blob's bytes ONCE. This is reused across every pattern -- decoding +
    Unicode-normalizing + casefolding a large file is O(file size) and must
    NOT be repeated per-pattern (that redundancy is the actual perf cost at
    repo scale, not the substring search itself)."""
    text = data.decode('utf-8', errors='ignore')
    forms = {'NFC': '', 'NFD': ''}
    if text:
        forms['NFC'] = unicodedata.normalize('NFC', text).casefold()
        forms['NFD'] = unicodedata.normalize('NFD', text).casefold()
    return forms


def _match_offsets(data: bytes, forms: dict, pattern: str) -> list[int]:
    """Return byte offsets in `data` where `pattern` appears in any of:
    literal UTF-8 bytes, the precomputed Unicode-normalized (NFC/NFD) +
    casefolded text forms, URL percent-encoding, HTML numeric-entity
    encoding, or a JS \\uXXXX escape. Offsets from the normalized-text pass
    are approximate (computed by re-encoding the prefix up to the match) but
    are always within the file."""
    offsets: list[int] = []

    # 1. Literal UTF-8 bytes (catches plain ASCII + raw Hebrew UTF-8 bytes).
    pat_bytes = pattern.encode('utf-8')
    if pat_bytes:
        start = 0
        while True:
            idx = data.find(pat_bytes, start)
            if idx == -1:
                break
            offsets.append(idx)
            start = idx + 1

    # 2. Precomputed Unicode-normalized (NFC + NFD) + casefolded text --
    #    catches a differently-composed Unicode form and case variants.
    for form in ('NFC', 'NFD'):
        norm_text = forms.get(form, '')
        if not norm_text:
            continue
        norm_pat = unicodedata.normalize(form, pattern).casefold()
        if not norm_pat:
            continue
        start = 0
        while True:
            idx = norm_text.find(norm_pat, start)
            if idx == -1:
                break
            byte_off = len(norm_text[:idx].encode('utf-8', errors='ignore'))
            offsets.append(byte_off)
            start = idx + 1

    # 3. URL percent-encoding.
    try:
        url_enc = urllib.parse.quote(pattern).encode('ascii')
        if url_enc and url_enc != pat_bytes:
            idx = data.find(url_enc)
            if idx != -1:
                offsets.append(idx)
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass

    # 4. HTML numeric-character-reference encoding (&#NNN; per char).
    html_enc = ''.join(f'&#{ord(c)};' for c in pattern).encode('ascii', errors='ignore')
    if html_enc:
        idx = data.find(html_enc)
        if idx != -1:
            offsets.append(idx)

    # 5. JS \uXXXX escape (per char).
    js_enc = ''.join(f'\\u{ord(c):04x}' for c in pattern).encode('ascii', errors='ignore')
    if js_enc:
        idx = data.find(js_enc)
        if idx != -1:
            offsets.append(idx)

    return offsets


def _scan_bytes(rel_path: str, data: bytes, patterns: list[str]) -> list[Issue]:
    """Scan one blob's bytes against every pattern; return Issues (never the
    matched text itself)."""
    issues = []
    forms = _build_search_forms(data)
    for idx, pattern in enumerate(patterns):
        for offset in _match_offsets(data, forms, pattern):
            issues.append(Issue(path=rel_path, offset=offset, pattern_index=idx))
    return issues


# ---------------------------------------------------------------------------
# git plumbing
# ---------------------------------------------------------------------------

def _git(args: list[str]) -> tuple[int, bytes, bytes]:
    proc = subprocess.run(['git'] + args, cwd=str(ROOT_DIR), capture_output=True)
    return proc.returncode, proc.stdout, proc.stderr


def _git_lines(args: list[str]) -> list[str]:
    rc, out, _ = _git(args)
    if rc != 0:
        return []
    text = out.decode('utf-8', errors='replace')
    return [line for line in text.splitlines() if line]


def _batch_read_git_objects(refs: list[str]) -> dict:
    """Bulk-read git blobs via `git cat-file --batch` (one subprocess for the
    whole repo instead of one per file). refs: e.g. 'HEAD:path/to/file' or
    ':path/to/file' (staged index). Returns {ref: bytes}; missing/unreadable
    refs are simply absent from the result."""
    if not refs:
        return {}
    proc = subprocess.Popen(
        ['git', 'cat-file', '--batch'],
        cwd=str(ROOT_DIR),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    input_data = ('\n'.join(refs) + '\n').encode('utf-8')
    out, _ = proc.communicate(input_data)

    result = {}
    pos = 0
    out_len = len(out)
    for ref in refs:
        nl = out.find(b'\n', pos)
        if nl == -1:
            break
        header = out[pos:nl].decode('utf-8', errors='replace')
        pos = nl + 1
        parts = header.split()
        if len(parts) != 3:
            # "<ref> missing" (2 tokens) or any malformed header -- no
            # content bytes follow for this ref; move on.
            continue
        try:
            size = int(parts[2])
        except ValueError:
            continue
        content = out[pos:pos + size]
        pos += size + 1  # skip the trailing newline git appends after content
        if pos > out_len:
            break
        result[ref] = content
    return result


# ---------------------------------------------------------------------------
# scan_repo -- three separate git surfaces
# ---------------------------------------------------------------------------

def scan_repo(patterns: list[str]) -> list[Issue]:
    """Scan HEAD/index blobs, tracked worktree files, and non-ignored
    untracked candidates. Each is a SEPARATE pass; a hit on any is a failure
    (a leak may differ between a staged blob and the worktree, or sit in an
    untracked file about to be committed)."""
    issues: list[Issue] = []

    tracked = _git_lines(['ls-files'])
    staged = _git_lines(['diff', '--cached', '--name-only'])

    # Pass 1: HEAD blobs + staged index blobs (bulk-read via cat-file --batch;
    # HEAD may be empty on a brand-new repo -- ls-tree returns [] gracefully).
    head_paths = _git_lines(['ls-tree', '-r', 'HEAD', '--name-only'])
    refs = [f'HEAD:{rel}' for rel in head_paths] + [f':{rel}' for rel in staged]
    blobs = _batch_read_git_objects(refs)
    for rel in head_paths:
        data = blobs.get(f'HEAD:{rel}')
        if data is not None:
            issues += _scan_bytes_literal(f'HEAD:{rel}', data, patterns)
    for rel in staged:
        data = blobs.get(f':{rel}')
        if data is not None:
            issues += _scan_bytes_literal(f'INDEX:{rel}', data, patterns)

    # Pass 2: tracked worktree files (read straight from disk).
    for rel in tracked:
        p = ROOT_DIR / rel
        if p.is_file():
            try:
                data = p.read_bytes()
            except OSError:
                continue
            issues += _scan_bytes_literal(rel, data, patterns)

    # Pass 3: non-ignored untracked candidates (files about to be `git add`ed
    # that are not yet tracked and not gitignored).
    for rel in _git_lines(['ls-files', '--others', '--exclude-standard']):
        p = ROOT_DIR / rel
        if p.is_file():
            try:
                data = p.read_bytes()
            except OSError:
                continue
            issues += _scan_bytes_literal(f'UNTRACKED:{rel}', data, patterns)

    return issues


# ---------------------------------------------------------------------------
# scan_asset -- a built product asset (file or directory), recursive
# ---------------------------------------------------------------------------

def _is_asset_file(p: Path) -> bool:
    name = p.name.lower()
    return name.endswith(_ASSET_SUFFIXES)


def scan_asset(path, patterns: list[str]) -> list[Issue]:
    """Scan a single file OR recursively walk a whole directory. A directory
    walk is restricted to asset-relevant suffixes (.bin/.bin.br/.json/.html);
    a single file argument is always scanned (the caller asked for it
    explicitly, regardless of its suffix)."""
    p = Path(path)
    issues: list[Issue] = []
    if not p.exists():
        return issues
    if p.is_file():
        candidates = [p]
    else:
        candidates = [c for c in p.rglob('*') if c.is_file() and _is_asset_file(c)]
    for c in candidates:
        try:
            data = c.read_bytes()
        except OSError:
            continue
        try:
            rel = str(c.relative_to(ROOT_DIR))
        except ValueError:
            rel = str(c)
        issues += _scan_bytes(rel, data, patterns)
    return issues


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _report(issues: list[Issue]) -> None:
    if not issues:
        print("check_atlas_masking: no matches -- clean.")
        return
    print(f"check_atlas_masking: {len(issues)} issue(s) found:")
    for issue in issues:
        print(f"  {issue.format()}")


def _run_self_test() -> int:
    """Quick internal smoke check (dev convenience -- the authoritative test
    suite is tests/test_atlas_masking_scan.py). Exercises the shared matcher
    against a synthetic, non-restricted pattern so this can run with no env
    var set."""
    pattern = "ZZZ_SELFTEST_TOKEN_ZZZ"
    ok = True

    plain = f"prefix {pattern} suffix".encode('utf-8')
    if not _scan_bytes('synthetic', plain, [pattern]):
        print("SELF-TEST FAIL: literal UTF-8 match not detected")
        ok = False

    url_form = urllib.parse.quote(pattern).encode('ascii')
    if not _scan_bytes('synthetic', b'x=' + url_form, [pattern]):
        print("SELF-TEST FAIL: URL-encoded match not detected")
        ok = False

    clean = b"nothing restricted here"
    if _scan_bytes('synthetic', clean, [pattern]):
        print("SELF-TEST FAIL: false positive on clean content")
        ok = False

    if ok:
        print("SELF-TEST PASS")
        return 0
    return 1


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scan-repo', action='store_true',
                        help='Scan HEAD/index blobs + tracked worktree + untracked candidates')
    parser.add_argument('--scan-asset', metavar='PATH', default=None,
                        help='Scan a built asset file or directory (recursive)')
    parser.add_argument('--self-test', action='store_true',
                        help='Run a quick internal smoke check with a synthetic pattern')
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    if args.self_test:
        return _run_self_test()

    if not args.scan_repo and not args.scan_asset:
        print("Nothing to do -- pass --scan-repo and/or --scan-asset PATH "
              "(or --self-test)", file=sys.stderr)
        return 1

    patterns = load_patterns()
    if not patterns:
        print(
            "ERROR: no masking patterns loaded. MASKING_SCAN_PATTERNS_FILE is "
            "unset, points at a missing file, or the file yielded zero "
            "patterns. Refusing to report a false-green scan -- set the env "
            "var to a gitignored local pattern file before running this scan.",
            file=sys.stderr,
        )
        return 1

    issues: list[Issue] = []
    if args.scan_repo:
        issues += scan_repo(patterns)
    if args.scan_asset:
        issues += scan_asset(args.scan_asset, patterns)

    _report(issues)
    return 0 if not issues else 1


if __name__ == '__main__':
    sys.exit(main())
