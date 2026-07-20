#!/usr/bin/env python3
"""
Masking Scan -- D-07 (Phase 133 forerunner of the permanent DATA-05 CI guard)

Scans for a restricted reference-corpus name/aliases (referred to everywhere
in committed material only by the internal codename "M-source") across:

  - `scan_repo()` -- three SEPARATE git surfaces enumerated with NUL-delimited
    (`-z`) plumbing and read BY OBJECT ID (never by `git show HEAD:path`, which
    breaks on `core.quotePath`/embedded-newline paths): HEAD blobs + staged
    index blobs (bulk `git cat-file --batch` by sha), tracked worktree files,
    and non-ignored untracked candidates. A hit on ANY surface is a failure,
    because a leak can differ between a staged blob and the worktree, or sit in
    an untracked file about to be committed. Every path component is ALSO
    scanned (a restricted term can hide in a file NAME); a matching path is
    redacted (opaque id) in output, never echoed.
  - `scan_asset(path)` -- a built product asset (a single file OR a whole
    directory, recursively), including Brotli (`.br`) payloads which are
    decompressed and scanned in BOTH their compressed and decompressed forms.

ONE semantically-complete, fail-CLOSED matcher runs on EVERY surface (repo and
asset alike): every restricted pattern is matched as literal bytes, as
Unicode-normalized (NFC/NFD) + casefolded variants, across explicitly supported
byte encodings (UTF-8 / UTF-16 / UTF-32 with BOM- and content-aware decoding),
and in URL, HTML-entity, and JS/JSON `\\u`/`\\x` escaped forms -- decoded back
into canonical text so that FULLY *and* PARTIALLY (mixed literal+escaped)
encoded leaks are caught. Performance over a large working tree is achieved with
the fast C `bytes.find` primitive, a single `unquote_to_bytes` URL buffer, and a
windowed de-escape around the (sparse) HTML/JS escape introducers -- coverage is
never traded for speed.

SECURITY CONTROLS (do not weaken):
  - NEVER hardcode the restricted patterns in this file. `load_patterns()`
    reads them from a gitignored local file referenced by the
    MASKING_SCAN_PATTERNS_FILE env var -- the same env-sourced-secret idiom
    as PUZZLE_UPLOAD_SECRET (web/puzzle_tokens.py).
  - FAIL-CLOSED: EVERY condition that could hide a leak is a hard error (raised
    as ScanError, converted to a non-zero exit by main()): an empty/absent
    pattern set at ANY public scan entry point; a non-zero git result; a
    missing expected blob or malformed `cat-file --batch` record; a file that
    stats-as-regular but cannot be read; a BOM-declared encoding that fails to
    decode; a Brotli payload that fails to decompress. The scan must NEVER
    exit 0 while any surface was silently lost.
  - NEVER ECHO: reported issues include only the (redacted-if-leaky) relative
    file path, an approximate byte offset, and a pattern INDEX -- never the
    matched pattern text, and never a path that itself contains a pattern.

Usage:
    python scripts/check_atlas_masking.py --scan-repo
    python scripts/check_atlas_masking.py --scan-asset atlas_data/
    python scripts/check_atlas_masking.py --scan-repo --scan-asset atlas_data/
    python scripts/check_atlas_masking.py --strict --scan-repo --scan-asset atlas_data/
    python scripts/check_atlas_masking.py --self-test
"""

import argparse
import html
import os
import re
import subprocess
import sys
import unicodedata
import urllib.parse
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent

# ---------------------------------------------------------------------------
# Tunables (all bound the work done; none affect coverage)
# ---------------------------------------------------------------------------
# Window (bytes) taken on each side of an HTML/JS escape introducer for the
# de-escape pass. A mixed literal+escaped occurrence of a pattern is bounded in
# length by max_pattern_chars * max_bytes_per_escaped_char; 2 KiB is generously
# larger than any plausible pattern's escaped span.
_WINDOW = 2048
# Above this many escape introducers in a single blob, de-escape the whole blob
# rather than windowing (windows would have merged to cover it anyway).
_DENSE_INTRO_LIMIT = 4096
# A blob is treated as (no-BOM) UTF-16/UTF-32 when at least this fraction of its
# bytes are NUL -- real wide-encoded ASCII text is ~50% NUL; scattered NULs in
# otherwise-8-bit content stay well below this.
_WIDE_NULL_RATIO = 0.20
# Files at/under this size are read whole; larger files are streamed in
# overlapping chunks so we never hold a giant file (or the whole tree) in RAM.
_WHOLE_READ_CAP = 256 * 1024 * 1024
_CHUNK_SIZE = 64 * 1024 * 1024
# Overlap must exceed the longest byte-form/window a match could straddle.
_CHUNK_OVERLAP = _WINDOW * 8

# Directory-walk suffix allowlist for a NON-strict `scan_asset` on a directory
# (a single explicit file argument is always scanned; --strict scans EVERY
# regular file regardless of suffix). Covers baked-atlas outputs plus the common
# text/markup/script surfaces a leak could ride in.
_ASSET_SUFFIXES = (
    '.bin', '.bin.br', '.br', '.json', '.html', '.htm',
    '.js', '.mjs', '.cjs', '.css', '.svg', '.xml', '.txt', '.map',
)


class ScanError(RuntimeError):
    """Raised on ANY fail-closed condition (see module docstring). main()
    converts an uncaught ScanError into a non-zero exit."""


@dataclass(frozen=True)
class Issue:
    """One masking hit. Deliberately carries NO pattern text and NO un-redacted
    path (never-echo). `path` is already redacted by the matcher when the path
    itself contains a restricted pattern."""
    path: str
    offset: int
    pattern_index: int
    surface: str = 'content'

    def format(self) -> str:
        return (f"MASK HIT [{self.surface}]: {self.path} "
                f"@byte {self.offset} (pattern #{self.pattern_index})")


# ---------------------------------------------------------------------------
# Pattern loading (fail-safe, env-sourced, gitignored file)
# ---------------------------------------------------------------------------

def load_patterns() -> list[str]:
    """Load restricted-string patterns from the gitignored file referenced by
    MASKING_SCAN_PATTERNS_FILE. Returns [] on ANY failure (unset env var,
    missing file, unreadable file, or an all-comment/blank file) -- callers
    MUST treat an empty return as a hard error, never a silent pass (see
    `_require_patterns`)."""
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
    return [
        line.strip() for line in raw.splitlines()
        if line.strip() and not line.strip().startswith('#')
    ]


def _require_patterns(patterns) -> list[str]:
    """Enforce a non-empty, validated pattern set at EVERY public scan entry
    point (HIGH-9). A zero-pattern scan is the canonical false-green and is a
    hard fail-closed error."""
    if not patterns:
        raise ScanError(
            "no masking patterns loaded -- refusing to run a zero-pattern "
            "(false-green) scan. Set MASKING_SCAN_PATTERNS_FILE to a gitignored "
            "local pattern file."
        )
    cleaned = [p for p in patterns if isinstance(p, str) and p]
    if not cleaned:
        raise ScanError("masking pattern set contained no usable (non-empty) patterns")
    return cleaned


# ---------------------------------------------------------------------------
# Brotli (fail-closed)
# ---------------------------------------------------------------------------

def _brotli_decompress(data: bytes) -> bytes:
    """Decompress a Brotli payload. A MISSING brotli library OR a failed
    decompression is fail-closed (ScanError) -- a `.br` whose decompressed
    bytes we cannot inspect could hide a leak (HIGH-4)."""
    try:
        import brotli  # noqa: PLC0415 (lazy: only needed for .br payloads)
    except ImportError as exc:
        raise ScanError(
            "brotli library unavailable -- cannot inspect a Brotli (.br) "
            "payload; refusing to pass it unscanned (fail-closed)."
        ) from exc
    try:
        return brotli.decompress(data)
    except Exception as exc:  # brotli raises brotli.error / generic
        raise ScanError("Brotli decompression failed (fail-closed)") from exc


# ---------------------------------------------------------------------------
# Escape-form encoders (pattern side) + de-escapers (content side)
# ---------------------------------------------------------------------------

def _url_form(v: str) -> bytes:
    return urllib.parse.quote(v, safe='').encode('ascii', 'ignore')


def _html_dec_form(v: str) -> bytes:
    return ''.join(f'&#{ord(c)};' for c in v).encode('ascii', 'ignore')


def _html_hex_form(v: str) -> bytes:
    return ''.join(f'&#x{ord(c):x};' for c in v).encode('ascii', 'ignore')


def _js_u_form(v: str) -> bytes:
    parts = []
    for c in v:
        o = ord(c)
        if o > 0xFFFF:  # non-BMP -> UTF-16 surrogate pair (HIGH-5)
            o2 = o - 0x10000
            hi = 0xD800 + (o2 >> 10)
            lo = 0xDC00 + (o2 & 0x3FF)
            parts.append(f'\\u{hi:04x}\\u{lo:04x}')
        else:
            parts.append(f'\\u{o:04x}')
    return ''.join(parts).encode('ascii', 'ignore')


def _js_x_form(v: str) -> bytes:
    if any(ord(c) > 0xFF for c in v):
        return b''
    return ''.join(f'\\x{ord(c):02x}' for c in v).encode('ascii', 'ignore')


# One combined JS/JSON escape de-escaper (a SINGLE sub pass rather than four --
# the per-call regex overhead over a large tree dominates otherwise). Alternation
# order matters: `\u{..}` and surrogate PAIRS are tried before a lone `\uXXXX`.
_JS_COMBINED = re.compile(
    r'\\u\{([0-9a-f]{1,6})\}'                       # 1: \u{HHHH}
    r'|\\u(d[89ab][0-9a-f]{2})\\u(d[c-f][0-9a-f]{2})'  # 2,3: UTF-16 surrogate pair
    r'|\\u([0-9a-f]{4})'                            # 4: \uXXXX
    r'|\\x([0-9a-f]{2})',                           # 5: \xXX
    re.IGNORECASE,
)
# Byte-level introducers whose PRESENCE gates the HTML/JS de-escape pass.
_HTML_JS_INTRODUCERS = (b'&#', b'\\u', b'\\U', b'\\x', b'\\X')


def _safe_chr(codepoint: int) -> str:
    try:
        return chr(codepoint)
    except (ValueError, OverflowError):
        return ''


def _combine_surrogates(hi_hex: str, lo_hex: str) -> str:
    hi = int(hi_hex, 16)
    lo = int(lo_hex, 16)
    cp = 0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
    return _safe_chr(cp)


def _js_repl(m: 're.Match') -> str:
    if m.group(1) is not None:
        return _safe_chr(int(m.group(1), 16))
    if m.group(2) is not None:
        return _combine_surrogates(m.group(2), m.group(3))
    if m.group(4) is not None:
        return _safe_chr(int(m.group(4), 16))
    if m.group(5) is not None:
        return _safe_chr(int(m.group(5), 16))
    return m.group(0)


def _deescape_js(text: str) -> str:
    """Decode JS/JSON string escapes (`\\u{..}`, surrogate pairs, `\\uXXXX`,
    `\\xXX`) into their canonical characters in ONE pass. Runs only on small
    windowed text that actually contains a backslash."""
    return _JS_COMBINED.sub(_js_repl, text)


def _deescape_html_js(text: str) -> str:
    """Decode HTML numeric/named entities AND JS/JSON string escapes into their
    canonical characters (HIGH-5). Runs only on small windowed text."""
    if '&' in text:
        text = html.unescape(text)  # &#NNN; &#xHH; &name;
    if '\\' in text:
        text = _deescape_js(text)
    return text


# ---------------------------------------------------------------------------
# The one semantically-complete matcher (used on EVERY surface)
# ---------------------------------------------------------------------------

def _norm_case_variants(pattern: str) -> list[str]:
    """The canonical text variants of a pattern: NFC/NFD normalization forms and
    their casefolds. Case-insensitivity for the byte passes is additionally
    achieved by lowercasing the data, so ASCII variants collapse there."""
    out: list[str] = []
    seen: set[str] = set()
    for norm in (pattern,
                 unicodedata.normalize('NFC', pattern),
                 unicodedata.normalize('NFD', pattern)):
        for v in (norm, norm.casefold()):
            if v and v not in seen:
                seen.add(v)
                out.append(v)
    return out


class PatternMatcher:
    """Precomputes every search form ONCE and applies them to a blob's bytes.

    Byte passes search `data.lower()` (ASCII-only fold, matching lowercased
    forms) so arbitrary ASCII case is covered in a single fast pass. Wide
    encodings (UTF-16/UTF-32) and mixed HTML/JS escapes are covered by
    content-aware decode + windowed de-escape passes."""

    def __init__(self, patterns):
        patterns = _require_patterns(patterns)
        self.n = len(patterns)
        # utf-8 byte forms (lowercased) -- the HOT content pass + URL/decoded
        # buffers. Fully-encoded (URL/HTML/JS) forms are NOT searched in the hot
        # content pass: the dedicated URL buffer (Pass 2) and windowed HTML/JS
        # de-escape (Pass 3) already catch both FULLY and PARTIALLY encoded
        # leaks -- carrying the encoded forms as extra full-tree find-passes
        # would multiply the cost several-fold for zero added coverage.
        self.utf8_forms: list[tuple[bytes, int]] = []
        # fully URL/HTML/JS-encoded ASCII forms -- used ONLY for scanning short
        # path strings (cheap) as defense-in-depth against an encoded filename.
        self.encoded_forms: list[tuple[bytes, int]] = []
        # casefolded str needles for the windowed-de-escape and decoded-text passes.
        self.text_needles: list[tuple[str, int]] = []
        # dynamic de-escape window: large enough that any pattern's fully-escaped
        # byte span fits inside a window anchored on any one of its introducers.
        max_escaped = 256
        for idx, pattern in enumerate(patterns):
            variants = _norm_case_variants(pattern)
            u8_seen: set[bytes] = set()
            enc_seen: set[bytes] = set()
            txt_seen: set[str] = set()
            for v in variants:
                u8 = v.encode('utf-8', 'ignore').lower()
                if u8 and u8 not in u8_seen:
                    u8_seen.add(u8)
                    self.utf8_forms.append((u8, idx))
                for form in (
                    _url_form(v).lower(),
                    _html_dec_form(v).lower(),
                    _html_hex_form(v).lower(),
                    _js_u_form(v).lower(),
                    _js_x_form(v).lower(),
                ):
                    if form:
                        max_escaped = max(max_escaped, len(form))
                        if form not in enc_seen and form not in u8_seen:
                            enc_seen.add(form)
                            self.encoded_forms.append((form, idx))
                cf = v.casefold()
                if cf and cf not in txt_seen:
                    txt_seen.add(cf)
                    self.text_needles.append((cf, idx))
        self.window = max(512, 2 * max_escaped)
        # If NO pattern itself contains a literal '%', the URL-decoded buffer is
        # a strict superset of the raw buffer for literal matching (unquote only
        # rewrites valid %XX triplets, copying everything else verbatim), so we
        # can search ONE buffer per file instead of two. If a pattern DOES carry
        # a '%', unquote could mangle a literal occurrence, so we search both.
        self._patterns_have_pct = any('%' in p for p in patterns)

    # -- path redaction / filename scanning (HIGH-8) ------------------------

    def path_hit_index(self, path: str):
        """Return a matching pattern index if the path string itself contains a
        restricted pattern (in any supported form), else None."""
        pb = path.encode('utf-8', 'surrogatepass' if _needs_surrogatepass(path) else 'ignore')
        pl = pb.lower()
        for form, idx in self.utf8_forms:
            if form in pl:
                return idx
        for form, idx in self.encoded_forms:
            if form in pl:
                return idx
        cf = path.casefold()
        for needle, idx in self.text_needles:
            if needle in cf:
                return idx
        return None

    def redact_path(self, path: str) -> str:
        """A path that itself contains a restricted pattern MUST NOT be echoed;
        replace it with an opaque, non-reversible id (HIGH-8)."""
        if self.path_hit_index(path) is not None:
            digest = sha256(path.encode('utf-8', 'surrogatepass'
                                       if _needs_surrogatepass(path) else 'ignore')).hexdigest()[:12]
            return f"<redacted-path:{digest}>"
        return path

    # -- content scanning ----------------------------------------------------

    def scan(self, data: bytes, rel_path: str) -> list[Issue]:
        """Apply the full matcher to one blob's bytes. `rel_path` MUST already
        be redaction-safe (callers pass the output of `redact_path`)."""
        if not data:
            return []
        issues: list[Issue] = []
        _seen_offsets: set[tuple[int, int, str]] = set()

        def _emit(offset: int, idx: int, surface: str):
            key = (offset, idx, surface)
            if key not in _seen_offsets:
                _seen_offsets.add(key)
                issues.append(Issue(path=rel_path, offset=offset,
                                    pattern_index=idx, surface=surface))

        def _find_forms(buf: bytes, surface: str):
            for form, idx in self.utf8_forms:
                pos = buf.find(form)
                while pos != -1:
                    _emit(pos, idx, surface)
                    pos = buf.find(form, pos + 1)

        # Passes 1+2 -- literal UTF-8 forms over the raw buffer AND (for files
        # carrying percent-escapes) the URL-decoded buffer. The URL-decoded
        # buffer subsumes the raw buffer for literal matching, so in the common
        # case we search exactly ONE buffer per file. Fully / partially
        # HTML/JS-encoded forms are covered by Pass 3.
        if b'%' in data:
            try:
                ub = urllib.parse.unquote_to_bytes(data).lower()
            except Exception:  # pragma: no cover - unquote_to_bytes is total
                ub = data.lower()
            _find_forms(ub, 'url')
            if self._patterns_have_pct:
                _find_forms(data.lower(), 'raw')
        else:
            _find_forms(data.lower(), 'raw')

        # Pass 3 -- windowed HTML/JS de-escape (fully AND mixed HTML/JS-escaped).
        self._scan_html_js(data, _emit)

        # Pass 4 -- content-aware decode for BOM'd / wide (UTF-16/32) text.
        self._scan_decoded_text(data, _emit)

        return issues

    def _scan_html_js(self, data: bytes, emit):
        # Cheap presence gate: only the introducers actually present are worth
        # locating (each find-loop that follows would otherwise scan the whole
        # blob for an absent introducer).
        present = [intro for intro in _HTML_JS_INTRODUCERS if intro in data]
        if not present:
            return
        positions: list[int] = []
        dense = False
        for intro in present:
            start = 0
            while True:
                i = data.find(intro, start)
                if i == -1:
                    break
                positions.append(i)
                start = i + len(intro)
                if len(positions) > _DENSE_INTRO_LIMIT:
                    dense = True
                    break
            if dense:
                break
        if not positions:
            return
        if dense:
            spans = [(0, len(data))]
        else:
            positions.sort()
            spans = _merge_windows(positions, len(data), self.window)
        for a, b in spans:
            text = data[a:b].decode('utf-8', 'replace')
            # Only run the de-escaper whose introducer is actually in the window.
            if '&' in text:
                text = html.unescape(text)
            if '\\' in text:
                text = _deescape_js(text)
            de = text.casefold()
            if not de:
                continue
            for needle, idx in self.text_needles:
                pos = de.find(needle)
                while pos != -1:
                    emit(a, idx, 'escape')
                    pos = de.find(needle, pos + 1)

    def _scan_decoded_text(self, data: bytes, emit):
        codec = _detect_bom(data)
        text = None
        if codec is not None:
            try:
                text = data.decode(codec)
            except UnicodeDecodeError as exc:
                # A file that DECLARES an encoding (BOM) but will not decode is
                # fail-closed -- it could hide a leak behind malformed bytes.
                raise ScanError(
                    f"BOM-declared {codec} content failed to decode (fail-closed)"
                ) from exc
        else:
            # Bounded null-ratio probe (a real BOM-less UTF-16/32 blob is ~50%
            # NUL uniformly, so a head sample is representative) -- avoids a
            # full-file NUL count on every blob.
            sample = data[:65536]
            if sample.count(b'\x00') / len(sample) >= _WIDE_NULL_RATIO:
                text = _decode_wide_no_bom(data)
        if not text:
            return
        cf = text.casefold()
        for needle, idx in self.text_needles:
            pos = cf.find(needle)
            while pos != -1:
                emit(pos, idx, 'decoded')
                pos = cf.find(needle, pos + 1)


def build_matcher(patterns) -> PatternMatcher:
    """Public constructor -- enforces the non-empty pattern set (HIGH-9)."""
    return PatternMatcher(patterns)


def _merge_windows(positions, length, radius):
    spans = []
    for p in positions:
        a = max(0, p - radius)
        b = min(length, p + radius)
        if spans and a <= spans[-1][1]:
            spans[-1] = (spans[-1][0], max(spans[-1][1], b))
        else:
            spans.append((a, b))
    return spans


def _detect_bom(data: bytes):
    if data[:3] == b'\xef\xbb\xbf':
        return 'utf-8-sig'
    if data[:4] == b'\xff\xfe\x00\x00':
        return 'utf-32'
    if data[:4] == b'\x00\x00\xfe\xff':
        return 'utf-32'
    if data[:2] == b'\xff\xfe':
        return 'utf-16'
    if data[:2] == b'\xfe\xff':
        return 'utf-16'
    return None


def _decode_wide_no_bom(data: bytes):
    """Best-effort decode of BOM-less UTF-16 content (the realistic wide-encoding
    leak vector). Picks the endianness that yields the fewest replacement
    characters; falls back to None if neither is plausibly text."""
    best = None
    best_bad = None
    for codec in ('utf-16-le', 'utf-16-be'):
        try:
            text = data.decode(codec, 'replace')
        except (LookupError, UnicodeDecodeError):
            continue
        bad = text.count('�')
        if best is None or bad < best_bad:
            best = text
            best_bad = bad
    return best


def _needs_surrogatepass(s: str) -> bool:
    return any('\ud800' <= c <= '\udfff' for c in s)


def _scan_blob(matcher: PatternMatcher, data: bytes, raw_path: str) -> list[Issue]:
    """Scan one in-memory blob. Redacts the display path if the path itself
    leaks and emits a dedicated filename Issue (HIGH-8). Streams the content in
    overlapping chunks if it exceeds the whole-read cap."""
    display = matcher.redact_path(raw_path)
    issues: list[Issue] = []
    path_idx = matcher.path_hit_index(raw_path)
    if path_idx is not None:
        issues.append(Issue(path=display, offset=-1, pattern_index=path_idx,
                            surface='filename'))
    if len(data) <= _WHOLE_READ_CAP:
        issues += matcher.scan(data, display)
    else:
        base = 0
        n = len(data)
        while base < n:
            end = min(n, base + _CHUNK_SIZE)
            chunk = data[base:end]
            for iss in matcher.scan(chunk, display):
                issues.append(Issue(path=display,
                                    offset=iss.offset + base if iss.offset >= 0 else iss.offset,
                                    pattern_index=iss.pattern_index,
                                    surface=iss.surface))
            if end >= n:
                break
            base = end - _CHUNK_OVERLAP
    # A Brotli blob (e.g. a committed atlas-*.bin.br) is decompressed and its
    # payload scanned too (HIGH-4) -- the compressed bytes were scanned above.
    if raw_path.rstrip('/').lower().endswith('.br'):
        issues += matcher.scan(_brotli_decompress(data), display + '::brotli-decompressed')
    return issues


# ---------------------------------------------------------------------------
# git plumbing (NUL-delimited, object-id batch, fail-closed)
# ---------------------------------------------------------------------------

def _git(args, *, check=True):
    proc = subprocess.run(['git'] + args, cwd=str(ROOT_DIR), capture_output=True)
    if check and proc.returncode != 0:
        err = proc.stderr.decode('utf-8', 'replace').strip()
        raise ScanError(f"git {' '.join(args)} failed (rc={proc.returncode}): {err}")
    return proc.returncode, proc.stdout, proc.stderr


def _git_z(args) -> list[bytes]:
    """Run a git plumbing command and split its NUL-delimited stdout (HIGH-3).
    Fail-closed on a non-zero result (HIGH-2)."""
    _, out, _ = _git(args, check=True)
    return [rec for rec in out.split(b'\x00') if rec]


def _head_exists() -> bool:
    rc, _, _ = _git(['rev-parse', '--verify', '--quiet', 'HEAD'], check=False)
    return rc == 0


def _parse_ls_tree_z(records):
    """Yield (sha_str, path_bytes) for blob entries of `git ls-tree -r -z`."""
    for rec in records:
        head, _, path = rec.partition(b'\t')
        parts = head.split(b' ')
        if len(parts) < 3:
            raise ScanError("malformed ls-tree record (fail-closed)")
        otype, sha = parts[1], parts[2]
        if otype != b'blob':
            continue
        if not path:
            raise ScanError("ls-tree record without a path (fail-closed)")
        yield sha.decode('ascii'), path


def _parse_ls_files_stage_z(records):
    """Yield (sha_str, path_bytes) for `git ls-files -s -z` index entries."""
    for rec in records:
        head, _, path = rec.partition(b'\t')
        parts = head.split(b' ')
        if len(parts) < 3:
            raise ScanError("malformed ls-files -s record (fail-closed)")
        sha = parts[1]
        if not path:
            raise ScanError("ls-files -s record without a path (fail-closed)")
        yield sha.decode('ascii'), path


def _batch_read_shas(shas) -> dict:
    """Bulk-read blob content by OBJECT ID via `git cat-file --batch` (HIGH-3).
    Fail-closed (ScanError) on a non-zero git exit, a `<sha> missing` record, a
    malformed header, or a truncated stream (HIGH-2)."""
    uniq = sorted(set(shas))
    if not uniq:
        return {}
    proc = subprocess.Popen(
        ['git', 'cat-file', '--batch'],
        cwd=str(ROOT_DIR),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    payload = ('\n'.join(uniq) + '\n').encode('ascii')
    out, err = proc.communicate(payload)
    if proc.returncode != 0:
        raise ScanError(
            f"git cat-file --batch failed (rc={proc.returncode}): "
            f"{err.decode('utf-8', 'replace').strip()}"
        )
    result: dict[str, bytes] = {}
    pos = 0
    out_len = len(out)
    for sha in uniq:
        nl = out.find(b'\n', pos)
        if nl == -1:
            raise ScanError("truncated cat-file --batch stream (fail-closed)")
        header = out[pos:nl]
        pos = nl + 1
        parts = header.split(b' ')
        if len(parts) == 2 and parts[1] == b'missing':
            raise ScanError("expected blob missing from cat-file --batch (fail-closed)")
        if len(parts) != 3:
            raise ScanError("malformed cat-file --batch header (fail-closed)")
        try:
            size = int(parts[2])
        except ValueError as exc:
            raise ScanError("non-integer size in cat-file --batch header (fail-closed)") from exc
        if pos + size + 1 > out_len:
            raise ScanError("cat-file --batch content shorter than declared size (fail-closed)")
        result[parts[0].decode('ascii')] = out[pos:pos + size]
        pos += size
        if out[pos:pos + 1] != b'\n':
            raise ScanError("cat-file --batch record not newline-terminated (fail-closed)")
        pos += 1
    return result


def _decode_path_for_fs(path_bytes: bytes) -> str:
    return path_bytes.decode('utf-8', 'surrogateescape')


def _display_path(prefix: str, path_bytes: bytes) -> str:
    return f"{prefix}{path_bytes.decode('utf-8', 'replace')}"


# ---------------------------------------------------------------------------
# scan_repo -- three separate git surfaces + filename scanning
# ---------------------------------------------------------------------------

def scan_repo(patterns) -> list[Issue]:
    """Scan HEAD/index blobs, tracked worktree files, and non-ignored untracked
    candidates as SEPARATE passes; a hit on ANY is a failure. Every path is also
    scanned for a leaky NAME. Fail-closed throughout (HIGH-1, HIGH-2, HIGH-3,
    HIGH-8, HIGH-9)."""
    matcher = build_matcher(patterns)
    issues: list[Issue] = []

    # Pass 1 -- HEAD blobs + staged index blobs, read BY OBJECT ID.
    blob_entries: list[tuple[str, bytes, str]] = []  # (sha, path_bytes, prefix)
    if _head_exists():
        for sha, path in _parse_ls_tree_z(_git_z(['ls-tree', '-r', '-z', 'HEAD'])):
            blob_entries.append((sha, path, 'HEAD:'))
    for sha, path in _parse_ls_files_stage_z(_git_z(['ls-files', '-s', '-z'])):
        blob_entries.append((sha, path, 'INDEX:'))
    blobs = _batch_read_shas([sha for sha, _, _ in blob_entries])
    for sha, path, prefix in blob_entries:
        if sha not in blobs:
            raise ScanError("expected blob absent after cat-file batch (fail-closed)")
        display = _display_path(prefix, path)
        issues += _scan_named(matcher, blobs[sha], display)

    # Pass 2 -- tracked worktree files (read from disk).
    for path in _git_z(['ls-files', '-z']):
        issues += _scan_worktree_file(matcher, path, prefix='')

    # Pass 3 -- non-ignored untracked candidates.
    for path in _git_z(['ls-files', '--others', '--exclude-standard', '-z']):
        issues += _scan_worktree_file(matcher, path, prefix='UNTRACKED:')

    return issues


def _scan_named(matcher: PatternMatcher, data: bytes, raw_display: str) -> list[Issue]:
    return _scan_blob(matcher, data, raw_display)


def _scan_worktree_file(matcher, path_bytes, *, prefix) -> list[Issue]:
    fs_path = _decode_path_for_fs(path_bytes)
    p = ROOT_DIR / fs_path
    raw_display = _display_path(prefix, path_bytes)
    issues: list[Issue] = []
    # Scan the filename itself even if the file cannot be read as content.
    path_idx = matcher.path_hit_index(raw_display)
    if path_idx is not None:
        issues.append(Issue(path=matcher.redact_path(raw_display), offset=-1,
                            pattern_index=path_idx, surface='filename'))
    try:
        if p.is_symlink():
            # A symlink's own bytes are its target text; scan that (cheap) but do
            # not follow it (avoids escaping the tree / cycles).
            target = os.readlink(p)
            issues += matcher.scan(target.encode('utf-8', 'surrogatepass'
                                                 if _needs_surrogatepass(target) else 'ignore'),
                                   matcher.redact_path(raw_display))
            return issues
        if not p.is_file():
            return issues  # sockets/fifos/etc. hold no committable content
        size = p.stat().st_size
    except OSError as exc:
        raise ScanError(f"cannot stat enumerated file (fail-closed): {matcher.redact_path(raw_display)}") from exc
    if size <= _WHOLE_READ_CAP:
        try:
            data = p.read_bytes()
        except OSError as exc:
            raise ScanError(
                f"cannot read enumerated file (fail-closed): {matcher.redact_path(raw_display)}"
            ) from exc
        issues += matcher.scan(data, matcher.redact_path(raw_display))
        issues += _scan_compressed_variants(matcher, p, data, matcher.redact_path(raw_display))
    else:
        issues += _scan_streamed_file(matcher, p, matcher.redact_path(raw_display))
    return issues


def _scan_streamed_file(matcher, p: Path, display: str) -> list[Issue]:
    issues: list[Issue] = []
    try:
        fh = p.open('rb')
    except OSError as exc:
        raise ScanError(f"cannot open enumerated file (fail-closed): {display}") from exc
    with fh:
        base = 0
        carry = b''
        while True:
            try:
                chunk = fh.read(_CHUNK_SIZE)
            except OSError as exc:
                raise ScanError(f"read error while streaming (fail-closed): {display}") from exc
            if not chunk:
                break
            buf = carry + chunk
            for iss in matcher.scan(buf, display):
                off = iss.offset + (base - len(carry)) if iss.offset >= 0 else iss.offset
                issues.append(Issue(path=display, offset=off,
                                    pattern_index=iss.pattern_index, surface=iss.surface))
            carry = buf[-_CHUNK_OVERLAP:]
            base += len(chunk)
    return issues


# ---------------------------------------------------------------------------
# scan_asset -- a built product asset (file or directory), recursive
# ---------------------------------------------------------------------------

def _is_asset_file(p: Path) -> bool:
    name = p.name.lower()
    return name.endswith(_ASSET_SUFFIXES)


def _scan_compressed_variants(matcher, p: Path, data: bytes, display: str) -> list[Issue]:
    """If the file is a Brotli payload, additionally scan its DECOMPRESSED bytes
    (HIGH-4). The compressed bytes were already scanned by the caller."""
    if p.name.lower().endswith('.br'):
        decompressed = _brotli_decompress(data)
        return matcher.scan(decompressed, display + '::brotli-decompressed')
    return []


def scan_asset(path, patterns, *, strict: bool = False) -> list[Issue]:
    """Scan a single file OR recursively walk a whole directory. In --strict
    mode the path MUST exist and be non-empty, EVERY regular file is scanned
    (no suffix filter, includes .js), and any traversal/read error is
    fail-closed (HIGH-7). In non-strict mode a directory walk is limited to
    asset-relevant suffixes; a single explicit file is always scanned."""
    matcher = build_matcher(patterns)
    p = Path(path)
    issues: list[Issue] = []

    if not p.exists():
        if strict:
            raise ScanError(f"--strict asset path does not exist (fail-closed): {path}")
        return issues

    if p.is_file():
        candidates = [p]
    else:
        if strict:
            candidates = sorted(c for c in p.rglob('*') if c.is_file())
            if not candidates:
                raise ScanError(f"--strict asset directory is empty (fail-closed): {path}")
        else:
            candidates = sorted(c for c in p.rglob('*') if c.is_file() and _is_asset_file(c))

    for c in candidates:
        try:
            rel = str(c.relative_to(ROOT_DIR))
        except ValueError:
            rel = str(c)
        display = matcher.redact_path(rel)
        try:
            size = c.stat().st_size
        except OSError as exc:
            if strict:
                raise ScanError(f"cannot stat asset file (fail-closed): {display}") from exc
            continue
        path_idx = matcher.path_hit_index(rel)
        if path_idx is not None:
            issues.append(Issue(path=display, offset=-1, pattern_index=path_idx,
                                surface='filename'))
        if size <= _WHOLE_READ_CAP:
            try:
                data = c.read_bytes()
            except OSError as exc:
                if strict:
                    raise ScanError(f"cannot read asset file (fail-closed): {display}") from exc
                continue
            issues += matcher.scan(data, display)
            issues += _scan_compressed_variants(matcher, c, data, display)
        else:
            issues += _scan_streamed_file(matcher, c, display)
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
    """Quick internal smoke check (dev convenience). Uses a synthetic,
    non-restricted pattern so it runs with NO env var set. It builds its OWN
    matcher from a fabricated pattern and can therefore NEVER satisfy a real
    scan invocation (HIGH-9)."""
    pattern = "ZZZ_SELFTEST_TOKEN_ZZZ"
    matcher = build_matcher([pattern])
    ok = True

    checks = [
        ("literal UTF-8", f"prefix {pattern} suffix".encode('utf-8')),
        ("URL-encoded", b'x=' + urllib.parse.quote(pattern, safe='').encode('ascii')),
        ("HTML-entity", ('<p>' + ''.join(f'&#{ord(c)};' for c in pattern) + '</p>').encode('ascii')),
        ("JS \\u escape", ('"' + ''.join(f'\\u{ord(c):04x}' for c in pattern) + '"').encode('ascii')),
        ("mixed literal+escape",
         (pattern[:3] + ''.join(f'\\u{ord(c):04x}' for c in pattern[3:])).encode('ascii')),
    ]
    for label, blob in checks:
        if not matcher.scan(blob, 'synthetic'):
            print(f"SELF-TEST FAIL: {label} match not detected")
            ok = False

    if matcher.scan(b"nothing restricted here", 'synthetic'):
        print("SELF-TEST FAIL: false positive on clean content")
        ok = False

    if ok:
        print("SELF-TEST PASS")
        return 0
    return 1


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="D-07 masking scan")
    parser.add_argument('--scan-repo', action='store_true',
                        help='Scan HEAD/index blobs + tracked worktree + untracked candidates')
    parser.add_argument('--scan-asset', metavar='PATH', default=None,
                        help='Scan a built asset file or directory (recursive)')
    parser.add_argument('--strict', action='store_true',
                        help='CI mode: require BOTH surfaces; scan every asset file; '
                             'fail on any traversal/read error')
    parser.add_argument('--self-test', action='store_true',
                        help='Run a quick internal smoke check with a synthetic pattern')
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    # HIGH-9: a self-test may never be mixed with a real scan invocation.
    if args.self_test:
        if args.scan_repo or args.scan_asset or args.strict:
            print("ERROR: --self-test cannot be combined with --scan-repo / "
                  "--scan-asset / --strict.", file=sys.stderr)
            return 2
        return _run_self_test()

    if args.strict and not (args.scan_repo and args.scan_asset):
        print("ERROR: --strict requires BOTH --scan-repo and --scan-asset PATH "
              "(HIGH-7).", file=sys.stderr)
        return 2

    if not args.scan_repo and not args.scan_asset:
        print("Nothing to do -- pass --scan-repo and/or --scan-asset PATH "
              "(or --self-test)", file=sys.stderr)
        return 2

    patterns = load_patterns()
    try:
        _require_patterns(patterns)
    except ScanError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    try:
        issues: list[Issue] = []
        if args.scan_repo:
            issues += scan_repo(patterns)
        if args.scan_asset:
            issues += scan_asset(args.scan_asset, patterns, strict=args.strict)
    except ScanError as exc:
        print(f"ERROR (fail-closed): {exc}", file=sys.stderr)
        return 1

    _report(issues)
    return 0 if not issues else 1


if __name__ == '__main__':
    sys.exit(main())
