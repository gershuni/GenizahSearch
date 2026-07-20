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
    ALWAYS fully decompressed and scanned in BOTH compressed and decompressed
    forms (never streamed raw).

ONE semantically-complete, fail-CLOSED matcher runs on EVERY surface (repo and
asset alike). Its single canonical pipeline is uniform for every byte source:

    for each candidate CHARACTER-DECODING of the bytes
        (UTF-8 always; the BOM-declared codec when a BOM is present -- decode
         failure there is fail-closed; and, for BOM-less input whose NUL-lane
         ratio looks wide, EVERY plausible wide decoding -- UTF-16LE, UTF-16BE,
         UTF-32LE, UTF-32BE -- so a hit in any candidate is a hit):
        for each UNESCAPE of that decoded text
        (identity; URL percent- AND form-decoded `+`->space; HTML entities;
         JS/JSON `\\u{..}` / surrogate-pair / `\\uXXXX` / `\\xXX`):
            Unicode `.casefold()` the text and match the casefolded needles.

Because the needles are precomputed as the casefold of the pattern's NFC and
NFD normalization forms, casefolding the haystack alone (no per-blob re-NFC)
collapses BOTH case and canonical-normalization differences -- so a non-ASCII
uppercase leak (which a plain `bytes.lower()` ASCII fold would miss entirely)
is caught. A fast C `bytes.find` pre-pass over `data.lower()` (and over the
URL/form byte-decodings) keeps the common literal/ASCII-case case cheap; the
canonical text pass runs whenever the buffer is non-ASCII, carries a percent,
carries an HTML/JS escape introducer, or looks wide -- i.e. wherever a
transformation could hide a leak. Coverage is NEVER traded for speed.

SECURITY CONTROLS (do not weaken):
  - NEVER hardcode the restricted patterns in this file. `load_patterns()`
    reads them from a gitignored local file referenced by the
    MASKING_SCAN_PATTERNS_FILE env var -- the same env-sourced-secret idiom
    as PUZZLE_UPLOAD_SECRET (web/puzzle_tokens.py).
  - FAIL-CLOSED: EVERY condition that could hide a leak is a hard error (raised
    as ScanError, converted to a non-zero exit by main()): an empty/absent
    pattern set at ANY public scan entry point; ANY operational git failure (a
    non-zero result that is NOT a proven unborn HEAD); a missing expected blob
    or malformed `cat-file --batch` record; an enumerated file whose metadata
    (`os.lstat`) or bytes cannot be read; a directory that cannot be walked
    (`os.scandir`); a BOM-declared encoding that fails to decode; a URL decoder
    failure; a Brotli payload that fails to decompress or exceeds a sane cap; a
    pattern too long to safely straddle a streaming-chunk boundary. The scan
    must NEVER exit 0 while any surface was silently lost.
  - NEVER ECHO: reported issues carry only the (redacted-if-leaky) relative
    file path, an approximate offset, and a pattern INDEX -- never the matched
    pattern text, and never a path that itself contains a pattern. `Issue.format`
    and EVERY diagnostic (including any surfaced subprocess context) are routed
    through a pattern-aware sanitizer; raw subprocess stderr and raw asset paths
    are never printed.

Usage:
    python scripts/check_atlas_masking.py --scan-repo
    python scripts/check_atlas_masking.py --scan-asset atlas_data/
    python scripts/check_atlas_masking.py --scan-repo --scan-asset atlas_data/
    python scripts/check_atlas_masking.py --strict --scan-repo --scan-asset atlas_data/
    python scripts/check_atlas_masking.py --self-test
"""

import argparse
import os
import re
import stat
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
# A blob is treated as (no-BOM) UTF-16/UTF-32 when at least this fraction of a
# head sample is NUL -- real wide-encoded ASCII text is ~50% NUL; scattered
# NULs in otherwise-8-bit content stay well below this.
_WIDE_NULL_RATIO = 0.20
# Every plausible BOM-less wide decoding is scanned (HIGH-2): a hit in any is a hit.
_WIDE_CODECS = ('utf-16-le', 'utf-16-be', 'utf-32-le', 'utf-32-be')
# Files at/under this size are read whole; larger NON-.br files are streamed in
# overlapping chunks so we never hold a giant file (or the whole tree) in RAM.
_WHOLE_READ_CAP = 256 * 1024 * 1024
_CHUNK_SIZE = 64 * 1024 * 1024
# Brotli payloads are ALWAYS fully read + decompressed, never streamed raw
# (HIGH-4). The project's assets are bounded (<6 MB per docs/specs); anything
# past these caps is fail-closed rather than trusted or streamed.
_BR_COMPRESSED_CAP = 64 * 1024 * 1024
_BR_DECOMPRESSED_CAP = 512 * 1024 * 1024
# Byte-level introducers whose PRESENCE gates the HTML/JS de-escape pass.
_HTML_JS_INTRODUCERS = (b'&#', b'\\u', b'\\U', b'\\x', b'\\X')
# The HTML/JS de-escape pass windows around each (sparse) escape introducer
# rather than de-escaping whole multi-gigabyte blobs; the window radius is
# recomputed per matcher from the longest escaped form. Above this many
# introducers in one blob, de-escape the whole thing (windows would have merged).
_DENSE_INTRO_LIMIT = 4096

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


# ---------------------------------------------------------------------------
# Pattern-aware diagnostic sanitizer (never-echo, HIGH-9)
# ---------------------------------------------------------------------------
# The most-recently-built matcher acts as the active redactor for ALL diagnostic
# strings (ScanError messages, reported paths, CLI output). Set by build_matcher.
_ACTIVE_MATCHER = None


def _sanitize(text) -> str:
    """Route any diagnostic string through the active matcher's redactor so a
    restricted pattern (or a path that embeds one) can never be echoed, even
    from an error message or a subprocess-context string (HIGH-9)."""
    s = str(text)
    matcher = _ACTIVE_MATCHER
    if matcher is None:
        return s
    return matcher.redact_diagnostic(s)


@dataclass(frozen=True)
class Issue:
    """One masking hit. Deliberately carries NO pattern text and NO un-redacted
    path (never-echo). `path` is redacted by the matcher when the path itself
    contains a restricted pattern; `format()` ALSO re-sanitizes defensively so a
    trusting caller cannot leak (HIGH-9)."""
    path: str
    offset: int
    pattern_index: int
    surface: str = 'content'

    def format(self) -> str:
        return (f"MASK HIT [{self.surface}]: {_sanitize(self.path)} "
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
# Brotli (fail-closed, always fully decompressed with caps -- HIGH-4)
# ---------------------------------------------------------------------------

def _brotli_decompress(data: bytes) -> bytes:
    """Decompress a Brotli payload incrementally with a decompressed-size cap.
    A MISSING brotli library, a failed decompression, OR a payload that expands
    beyond `_BR_DECOMPRESSED_CAP` is fail-closed (ScanError) -- a `.br` whose
    decompressed bytes we cannot fully + safely inspect could hide a leak."""
    try:
        import brotli  # noqa: PLC0415 (lazy: only needed for .br payloads)
    except ImportError as exc:
        raise ScanError(
            "brotli library unavailable -- cannot inspect a Brotli (.br) "
            "payload; refusing to pass it unscanned (fail-closed)."
        ) from exc

    decompressor_cls = getattr(brotli, 'Decompressor', None)
    try:
        if decompressor_cls is not None:
            dec = decompressor_cls()
            feed = getattr(dec, 'process', None) or getattr(dec, 'decompress')
            out = bytearray()
            step = 1 << 20
            for i in range(0, len(data), step):
                out += feed(data[i:i + step])
                if len(out) > _BR_DECOMPRESSED_CAP:
                    raise ScanError(
                        "Brotli payload decompressed beyond the sane cap (fail-closed)"
                    )
            result = bytes(out)
        else:  # binding without an incremental decompressor
            result = brotli.decompress(data)
            if len(result) > _BR_DECOMPRESSED_CAP:
                raise ScanError(
                    "Brotli payload decompressed beyond the sane cap (fail-closed)"
                )
    except ScanError:
        raise
    except Exception as exc:  # brotli raises brotli.error / generic
        raise ScanError("Brotli decompression failed (fail-closed)") from exc
    return result


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
        if o > 0xFFFF:  # non-BMP -> UTF-16 surrogate pair
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


# HTML NUMERIC character references only (`&#NNN;` / `&#xHH;`). Masking patterns
# are corpus names/sigla, never HTML named entities, so we deliberately do NOT
# use `html.unescape` here: its pure-Python named-entity machinery fires a
# callback on EVERY `&` and turns a de-escape of binary content into a
# multi-second-per-megabyte pathology. This C-scanned regex only invokes its
# callback on a genuine numeric ref.
_HTML_NUMREF = re.compile(r'&#(?:x([0-9a-f]+)|([0-9]+));?', re.IGNORECASE)


def _html_numref_repl(m: 're.Match') -> str:
    if m.group(1) is not None:  # &#xHH; hexadecimal
        return _safe_chr(int(m.group(1), 16))
    return _safe_chr(int(m.group(2)))  # &#NNN; decimal (base 10)


# One combined JS/JSON escape de-escaper (a SINGLE sub pass rather than four).
# Alternation order matters: `\u{..}` and surrogate PAIRS are tried before a
# lone `\uXXXX`.
_JS_COMBINED = re.compile(
    r'\\u\{([0-9a-f]{1,6})\}'                            # 1: \u{HHHH}
    r'|\\u(d[89ab][0-9a-f]{2})\\u(d[c-f][0-9a-f]{2})'   # 2,3: UTF-16 surrogate pair
    r'|\\u([0-9a-f]{4})'                                # 4: \uXXXX
    r'|\\x([0-9a-f]{2})',                               # 5: \xXX
    re.IGNORECASE,
)


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
    `\\xXX`) into their canonical characters in ONE pass."""
    return _JS_COMBINED.sub(_js_repl, text)


def _deescape_html_js(text: str) -> str:
    """Decode HTML numeric char refs AND JS/JSON string escapes into their
    canonical characters. Handles mixed literal+escaped occurrences uniformly."""
    if '&#' in text:
        text = _HTML_NUMREF.sub(_html_numref_repl, text)
    if '\\' in text:
        text = _deescape_js(text)
    return text


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _norm_case_variants(pattern: str) -> list[str]:
    """The canonical text variants of a pattern: raw + NFC/NFD normalization
    forms and their casefolds. Used for the escaped-form encoders and the
    casefolded text needles."""
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


def _byte_form_variants(pattern: str) -> list[str]:
    """Exhaustive case + normalization variants of a pattern for the fast
    `bytes.find` pass (HIGH-1). Because `bytes.lower()` folds ONLY ASCII, a
    non-ASCII leak written in upper/title/casefold form would slip past a
    lowercase-only byte needle; precomputing every case rendition under BOTH
    NFC and NFD (the two standard normalization forms) lets the hot byte pass
    catch non-ASCII case/normalization variants for EVERY file -- without
    Unicode-casefolding multi-gigabyte haystacks (which is CI-prohibitive). The
    rarer wide (UTF-16/32) and escaped surfaces additionally casefold their
    (small / rare) decoded text directly."""
    forms: set[str] = set()
    for base in (pattern,
                 unicodedata.normalize('NFC', pattern),
                 unicodedata.normalize('NFD', pattern)):
        for cased in (base, base.casefold(), base.upper(), base.lower(), base.title()):
            for norm in (cased,
                         unicodedata.normalize('NFC', cased),
                         unicodedata.normalize('NFD', cased)):
                if norm:
                    forms.add(norm)
    return list(forms)


def _needs_surrogatepass(s: str) -> bool:
    return any('\ud800' <= c <= '\udfff' for c in s)


def _encode_text(s: str) -> bytes:
    return s.encode('utf-8', 'surrogatepass' if _needs_surrogatepass(s) else 'ignore')


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


def _looks_wide(data: bytes) -> bool:
    sample = data[:65536]
    if not sample:
        return False
    return sample.count(b'\x00') / len(sample) >= _WIDE_NULL_RATIO


def _align4(n: int) -> int:
    return (n + 3) & ~3


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


# ---------------------------------------------------------------------------
# The one semantically-complete matcher (used on EVERY surface)
# ---------------------------------------------------------------------------

class PatternMatcher:
    """Precomputes every search form ONCE and applies the single canonical
    pipeline (decode -> unescape -> casefold -> match) to a blob's bytes."""

    def __init__(self, patterns):
        patterns = _require_patterns(patterns)
        self.n = len(patterns)
        # Literal UTF-8 byte forms (ASCII-lowered) -- the fast `bytes.find`
        # pre-pass over the raw and URL/form byte-decodings.
        self.utf8_forms: list[tuple[bytes, int]] = []
        # Fully URL/HTML/JS-encoded ASCII forms -- used ONLY as a cheap
        # defense-in-depth pre-check when scanning short path strings.
        self.encoded_forms: list[tuple[bytes, int]] = []
        # Casefolded str needles for the canonical text pass (include the
        # casefold of both the NFC and NFD normalization forms, so casefolding
        # the haystack alone bridges case AND normalization -- HIGH-1).
        self.text_needles: list[tuple[str, int]] = []
        max_form_bytes = 1
        for idx, pattern in enumerate(patterns):
            u8_seen: set[bytes] = set()
            enc_seen: set[bytes] = set()
            txt_seen: set[str] = set()
            # Fast-pass byte forms: exhaustive case + NFC/NFD variants (HIGH-1).
            for v in _byte_form_variants(pattern):
                u8 = v.encode('utf-8', 'ignore').lower()
                if u8 and u8 not in u8_seen:
                    u8_seen.add(u8)
                    self.utf8_forms.append((u8, idx))
                    max_form_bytes = max(max_form_bytes, len(u8))
            # Escaped-form encoders (for the de-escape composition) + casefolded
            # text needles (for the wide/BOM + de-escape passes).
            for v in _norm_case_variants(pattern):
                for form in (
                    _url_form(v).lower(),
                    _html_dec_form(v).lower(),
                    _html_hex_form(v).lower(),
                    _js_u_form(v).lower(),
                    _js_x_form(v).lower(),
                ):
                    if form and form not in enc_seen and form not in u8_seen:
                        enc_seen.add(form)
                        self.encoded_forms.append((form, idx))
                        max_form_bytes = max(max_form_bytes, len(form))
                cf = v.casefold()
                if cf and cf not in txt_seen:
                    txt_seen.add(cf)
                    self.text_needles.append((cf, idx))
        # Window radius for the de-escape pass: large enough that any pattern's
        # fully-escaped byte span fits inside a window anchored on one introducer.
        self.window = max(512, 2 * max_form_bytes)
        # Worst-case matched byte span: any ASCII escaped form (or UTF-8 form)
        # could additionally be stored in a wide encoding (up to *4 bytes/unit),
        # so a straddling streaming match must fit inside that window.
        self.max_form_bytes = max_form_bytes * 4
        self.stream_overlap = max(4096, _align4(self.max_form_bytes - 1))
        global _ACTIVE_MATCHER
        _ACTIVE_MATCHER = self

    # -- redaction (never-echo, HIGH-8 / HIGH-9) ----------------------------

    def _text_hit_index(self, s: str):
        """Return a matching pattern index if the string `s` contains a
        restricted pattern in ANY supported form (literal, encoded, or via
        URL/HTML/JS unescape + casefold), else None. Used for leaky filenames
        AND for sanitizing arbitrary diagnostic strings."""
        if not s:
            return None
        low = _encode_text(s).lower()
        for form, idx in self.utf8_forms:
            if form in low:
                return idx
        for form, idx in self.encoded_forms:
            if form in low:
                return idx
        # Canonical: casefold the string itself, then its unescaped variants
        # (a partially-escaped name is caught here -- HIGH-9).
        variants = [s]
        if '%' in s or '+' in s:
            try:
                variants.append(urllib.parse.unquote(s))
                variants.append(urllib.parse.unquote_plus(s))
            except (UnicodeDecodeError, ValueError):
                pass  # best-effort on names; content passes are the fail-closed ones
        if '&' in s or '\\' in s:
            variants.append(_deescape_html_js(s))
        for variant in variants:
            cf = variant.casefold()
            for needle, idx in self.text_needles:
                if needle in cf:
                    return idx
        return None

    def path_hit_index(self, path: str):
        """Public: return a matching pattern index if the path itself is leaky."""
        return self._text_hit_index(path)

    def redact_path(self, path: str) -> str:
        """A path that itself contains a restricted pattern MUST NOT be echoed;
        replace it with an opaque, non-reversible id (HIGH-8)."""
        if self._text_hit_index(path) is not None:
            return f"<redacted-path:{sha256(_encode_text(path)).hexdigest()[:12]}>"
        return path

    def redact_diagnostic(self, s: str) -> str:
        """Redact ANY diagnostic string that embeds a restricted pattern
        (HIGH-9). Idempotent for already-redacted placeholders."""
        if self._text_hit_index(s) is not None:
            return f"<redacted:{sha256(_encode_text(s)).hexdigest()[:12]}>"
        return s

    # -- content scanning ---------------------------------------------------

    def scan(self, data: bytes, rel_path: str, *, stream_mode: bool = False) -> list[Issue]:
        """Apply the full canonical pipeline to one blob's bytes. `rel_path`
        MUST already be redaction-safe (callers pass the output of `redact_path`).
        `stream_mode` relaxes BOM strict-decode fail-closure to tolerate a chunk
        boundary that splits a code unit (the caller carries an overlap window
        wide enough to re-capture any straddling match -- HIGH-4)."""
        if not data:
            return []
        issues: list[Issue] = []
        seen: set[tuple[int, int, str]] = set()

        def emit(offset: int, idx: int, surface: str):
            key = (offset, idx, surface)
            if key not in seen:
                seen.add(key)
                issues.append(Issue(path=rel_path, offset=offset,
                                    pattern_index=idx, surface=surface))

        # Fast literal byte pass over the raw bytes (covers literal + ASCII-case
        # + the exhaustive non-ASCII case/normalization forms -- HIGH-1).
        self._find_bytes(data.lower(), self.utf8_forms, 'raw', emit)

        # URL/form byte-decodings (byte-level `unquote_to_bytes` is total -- no
        # false-failure on stray `%XX`; a defensive failure is still fail-CLOSED,
        # never fail-open -- HIGH-5). A URL/form-encoded leak lives in TEXT (a
        # query string / attribute / JSON value), which never contains NUL; NUL
        # marks binary content where `%`/`+` are just stray bytes, so decoding it
        # would be pure wasted work. The byte pass alone suffices on the decoded
        # buffers (their `%` are already resolved to literal bytes).
        if b'\x00' not in data and (b'%' in data or b'+' in data):
            for buf in self._url_byte_buffers(data):
                self._find_bytes(buf.lower(), self.utf8_forms, 'url', emit)

        # Canonical wide/BOM decode + windowed HTML/JS de-escape over the raw bytes.
        self._scan_canonical(data, 'decoded', emit, stream_mode=stream_mode)

        return issues

    def _url_byte_buffers(self, data: bytes) -> list[bytes]:
        out: list[bytes] = []
        try:
            if b'%' in data:
                out.append(urllib.parse.unquote_to_bytes(data))
            if b'+' in data:
                out.append(urllib.parse.unquote_to_bytes(data.replace(b'+', b' ')))
        except Exception as exc:  # unquote_to_bytes is total; defensive fail-closed (HIGH-5)
            raise ScanError("URL byte-decoding failed (fail-closed)") from exc
        return out

    def _find_bytes(self, low: bytes, forms, surface: str, emit):
        for form, idx in forms:
            pos = low.find(form)
            while pos != -1:
                emit(pos, idx, surface)
                pos = low.find(form, pos + 1)

    def _scan_canonical(self, buf: bytes, decoded_surface: str, emit, *, stream_mode: bool):
        # (1) Wide (UTF-16/UTF-32) and BOM-declared text -- RARE, so their
        # decoded string is casefolded and de-escaped whole (escapes compose with
        # the wide decoding -- HIGH-3). A BOM that will not decode is fail-closed;
        # BOM-less wide input is scanned under EVERY plausible codec (HIGH-2).
        for text in self._iter_wide_texts(buf, stream_mode=stream_mode):
            if not text:
                continue
            self._match_casefold(text, decoded_surface, emit)
            if '&' in text or '\\' in text:
                self._match_casefold(_deescape_html_js(text), 'escape', emit)
        # (2) HTML/JS escapes in the UTF-8 view -- de-escaped in bounded windows
        # anchored on the (sparse) introducers, then casefolded and matched
        # (catches FULLY and PARTIALLY escaped leaks without whole-blob work).
        self._scan_windowed_escapes(buf, emit)

    def _iter_wide_texts(self, data: bytes, *, stream_mode: bool):
        """Yield each WIDE/BOM character-decoding of `data` (HIGH-2, HIGH-3).
        The plain UTF-8 view is covered by the exhaustive byte-form pass and the
        windowed de-escape, so it is NOT re-casefolded whole here."""
        codec = _detect_bom(data)
        if codec is not None:
            if stream_mode:
                # A chunk boundary can split a wide code unit mid-stream; decode
                # leniently (the overlap window re-captures straddling matches)
                # rather than fail-closing on benign boundary misalignment.
                yield data.decode(codec, 'replace')
            else:
                try:
                    yield data.decode(codec)
                except UnicodeDecodeError as exc:
                    raise ScanError(
                        f"BOM-declared {codec} content failed to decode (fail-closed)"
                    ) from exc
        elif _looks_wide(data):
            for wide_codec in _WIDE_CODECS:
                yield data.decode(wide_codec, 'replace')

    def _scan_windowed_escapes(self, data: bytes, emit):
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
            if '&#' in text:
                text = _HTML_NUMREF.sub(_html_numref_repl, text)
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

    def _match_casefold(self, text: str, surface: str, emit):
        cf = text.casefold()
        for needle, idx in self.text_needles:
            pos = cf.find(needle)
            while pos != -1:
                emit(pos, idx, surface)
                pos = cf.find(needle, pos + 1)


def build_matcher(patterns) -> PatternMatcher:
    """Public constructor -- enforces the non-empty pattern set (HIGH-9) and
    registers the matcher as the active diagnostic redactor."""
    return PatternMatcher(patterns)


# ---------------------------------------------------------------------------
# git plumbing (NUL-delimited, object-id batch, fail-closed)
# ---------------------------------------------------------------------------

def _git(args, *, check=True):
    """Run git. Raise ScanError on a non-zero result when `check` (never echoing
    raw stderr -- HIGH-9). Returns (returncode, stdout, stderr)."""
    proc = subprocess.run(['git'] + args, cwd=str(ROOT_DIR), capture_output=True)
    if check and proc.returncode != 0:
        raise ScanError(f"git {' '.join(args)} failed (rc={proc.returncode}) (fail-closed)")
    return proc.returncode, proc.stdout, proc.stderr


def _git_z(args) -> list[bytes]:
    """Run a git plumbing command and split its NUL-delimited stdout.
    Fail-closed on a non-zero result."""
    _, out, _ = _git(args, check=True)
    return [rec for rec in out.split(b'\x00') if rec]


def _head_exists() -> bool:
    """True if HEAD resolves. A PROVEN unborn HEAD (a real work tree with zero
    commits) returns False; ANY other non-zero result is an operational error
    and is fail-closed (HIGH-7)."""
    rc, _, _ = _git(['rev-parse', '--verify', '--quiet', 'HEAD'], check=False)
    if rc == 0:
        return True
    rc2, out2, _ = _git(['rev-parse', '--is-inside-work-tree'], check=False)
    if rc2 == 0 and out2.strip() == b'true':
        return False  # proven unborn HEAD in a real work tree
    raise ScanError("git HEAD probe failed operationally (not a work tree) -- fail-closed")


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
    """Bulk-read blob content by OBJECT ID via `git cat-file --batch`.
    Fail-closed (ScanError) on a non-zero git exit, a `<sha> missing` record, a
    malformed header, or a truncated stream (never echoing raw stderr -- HIGH-9)."""
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
    out, _err = proc.communicate(payload)
    if proc.returncode != 0:
        raise ScanError(f"git cat-file --batch failed (rc={proc.returncode}) (fail-closed)")
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
# Shared byte / streaming scanning helpers
# ---------------------------------------------------------------------------

def _is_brotli(name: str) -> bool:
    return name.rstrip('/').lower().endswith('.br')


def _scan_bytes_maybe_brotli(matcher: PatternMatcher, data: bytes, display: str,
                             *, is_br: bool) -> list[Issue]:
    """Scan an in-memory blob. Brotli payloads are ALWAYS fully decompressed and
    both forms scanned (HIGH-4a); large non-.br blobs stream with a
    pattern-length overlap (HIGH-4b)."""
    issues: list[Issue] = []
    if is_br:
        if len(data) > _BR_COMPRESSED_CAP:
            raise ScanError("Brotli payload exceeds the compressed size cap (fail-closed)")
        issues += matcher.scan(data, display)
        issues += matcher.scan(_brotli_decompress(data), display + '::brotli-decompressed')
        return issues
    if len(data) <= _WHOLE_READ_CAP:
        issues += matcher.scan(data, display)
    else:
        issues += _scan_big_bytes(matcher, data, display)
    return issues


def _stream_guard(matcher: PatternMatcher) -> int:
    overlap = matcher.stream_overlap
    if matcher.max_form_bytes - 1 > _CHUNK_SIZE // 2:
        raise ScanError("a masking pattern is too long to safely stream (fail-closed)")
    return overlap


def _scan_big_bytes(matcher: PatternMatcher, data: bytes, display: str) -> list[Issue]:
    overlap = _stream_guard(matcher)
    issues: list[Issue] = []
    seen: set[tuple[int, int, str]] = set()
    base = 0
    n = len(data)
    while base < n:
        end = min(n, base + _CHUNK_SIZE)
        start = max(0, base - overlap)
        buf = data[start:end]
        for iss in matcher.scan(buf, display, stream_mode=True):
            off = iss.offset + start if iss.offset >= 0 else iss.offset
            key = (off, iss.pattern_index, iss.surface)
            if key in seen:
                continue
            seen.add(key)
            issues.append(Issue(path=display, offset=off,
                                pattern_index=iss.pattern_index, surface=iss.surface))
        base = end
    return issues


def _scan_streamed_file(matcher: PatternMatcher, p: Path, display: str) -> list[Issue]:
    overlap = _stream_guard(matcher)
    issues: list[Issue] = []
    seen: set[tuple[int, int, str]] = set()
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
            boff = base - len(carry)
            for iss in matcher.scan(buf, display, stream_mode=True):
                off = iss.offset + boff if iss.offset >= 0 else iss.offset
                key = (off, iss.pattern_index, iss.surface)
                if key in seen:
                    continue
                seen.add(key)
                issues.append(Issue(path=display, offset=off,
                                    pattern_index=iss.pattern_index, surface=iss.surface))
            carry = buf[-overlap:] if overlap else b''
            base += len(chunk)
    return issues


def _read_bytes_or_fail(p: Path, display: str) -> bytes:
    try:
        return p.read_bytes()
    except OSError as exc:
        raise ScanError(f"cannot read enumerated file (fail-closed): {display}") from exc


# ---------------------------------------------------------------------------
# scan_repo -- three separate git surfaces + filename scanning
# ---------------------------------------------------------------------------

def scan_repo(patterns) -> list[Issue]:
    """Scan HEAD/index blobs, tracked worktree files, and non-ignored untracked
    candidates as SEPARATE passes; a hit on ANY is a failure. Every path is also
    scanned for a leaky NAME. Fail-closed throughout."""
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
        raw_display = _display_path(prefix, path)
        display = matcher.redact_path(raw_display)
        idx = matcher.path_hit_index(raw_display)
        if idx is not None:
            issues.append(Issue(path=display, offset=-1, pattern_index=idx, surface='filename'))
        issues += _scan_bytes_maybe_brotli(matcher, blobs[sha], display,
                                           is_br=_is_brotli(raw_display))

    # Pass 2 -- tracked worktree files (read from disk).
    for path in _git_z(['ls-files', '-z']):
        issues += _scan_worktree_file(matcher, path, prefix='')

    # Pass 3 -- non-ignored untracked candidates.
    for path in _git_z(['ls-files', '--others', '--exclude-standard', '-z']):
        issues += _scan_worktree_file(matcher, path, prefix='UNTRACKED:')

    return issues


def _scan_worktree_file(matcher, path_bytes, *, prefix) -> list[Issue]:
    fs_path = _decode_path_for_fs(path_bytes)
    p = ROOT_DIR / fs_path
    raw_display = _display_path(prefix, path_bytes)
    display = matcher.redact_path(raw_display)
    issues: list[Issue] = []
    # Scan the filename itself even if the file cannot be read as content.
    idx = matcher.path_hit_index(raw_display)
    if idx is not None:
        issues.append(Issue(path=display, offset=-1, pattern_index=idx, surface='filename'))
    # Explicit os.lstat (never follows symlinks; raises on any metadata error -- HIGH-8).
    try:
        st = os.lstat(p)
    except OSError as exc:
        raise ScanError(f"cannot stat enumerated file (fail-closed): {display}") from exc
    mode = st.st_mode
    if stat.S_ISLNK(mode):
        # Scan the symlink's own target-path bytes; do NOT follow it (avoids
        # escaping the tree / cycles -- HIGH-8).
        try:
            target = os.readlink(p)
        except OSError as exc:
            raise ScanError(f"cannot read symlink (fail-closed): {display}") from exc
        issues += matcher.scan(_encode_text(target), display)
        return issues
    if not stat.S_ISREG(mode):
        return issues  # sockets/fifos/etc. hold no committable content
    if _is_brotli(fs_path):
        if st.st_size > _BR_COMPRESSED_CAP:
            raise ScanError("Brotli file exceeds the compressed size cap (fail-closed)")
        data = _read_bytes_or_fail(p, display)
        issues += _scan_bytes_maybe_brotli(matcher, data, display, is_br=True)
        return issues
    if st.st_size <= _WHOLE_READ_CAP:
        data = _read_bytes_or_fail(p, display)
        issues += matcher.scan(data, display)
    else:
        issues += _scan_streamed_file(matcher, p, display)
    return issues


# ---------------------------------------------------------------------------
# scan_asset -- a built product asset (file or directory), recursive
# ---------------------------------------------------------------------------

def _is_asset_file(p: Path) -> bool:
    return p.name.lower().endswith(_ASSET_SUFFIXES)


def _rel_display(c: Path) -> str:
    try:
        return str(c.relative_to(ROOT_DIR))
    except ValueError:
        return str(c)


def _walk_entries(root: Path):
    """Recursively yield (Path, kind) under `root` via os.scandir, raising
    ScanError on ANY enumeration/classification error (HIGH-8). kind is
    'file' or 'symlink'; symlinks are yielded (never followed into)."""
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                entries = list(it)
        except OSError as exc:
            raise ScanError(
                f"cannot enumerate asset directory (fail-closed): {_sanitize(d)}"
            ) from exc
        for e in entries:
            try:
                if e.is_symlink():
                    yield Path(e.path), 'symlink'
                elif e.is_dir(follow_symlinks=False):
                    stack.append(Path(e.path))
                elif e.is_file(follow_symlinks=False):
                    yield Path(e.path), 'file'
            except OSError as exc:
                raise ScanError(
                    f"cannot classify asset entry (fail-closed): {_sanitize(e.path)}"
                ) from exc


def scan_asset(path, patterns, *, strict: bool = False) -> list[Issue]:
    """Scan a single file OR recursively walk a whole directory. In --strict
    mode the path MUST exist and be non-empty, EVERY regular file is scanned
    (no suffix filter), and any traversal/read/metadata error is fail-closed
    (HIGH-7/HIGH-8). In non-strict mode a directory walk is limited to
    asset-relevant suffixes; a single explicit file is always scanned. Brotli
    payloads are always fully decompressed + scanned (HIGH-4a). Symlinks are
    never followed -- only their link text is scanned (HIGH-8)."""
    matcher = build_matcher(patterns)
    p = Path(path)
    issues: list[Issue] = []

    try:
        top = os.lstat(p)
        exists = True
    except FileNotFoundError:
        exists = False
    except OSError as exc:
        raise ScanError(
            f"cannot stat asset path (fail-closed): {matcher.redact_diagnostic(str(path))}"
        ) from exc

    if not exists:
        if strict:
            raise ScanError(
                f"--strict asset path does not exist (fail-closed): "
                f"{matcher.redact_diagnostic(str(path))}"
            )
        return issues

    if stat.S_ISREG(top.st_mode):
        candidates: list[tuple[Path, str]] = [(p, 'file')]
    elif stat.S_ISLNK(top.st_mode):
        candidates = [(p, 'symlink')]
    elif stat.S_ISDIR(top.st_mode):
        walked = list(_walk_entries(p))
        if not strict:
            walked = [(c, k) for (c, k) in walked if k == 'symlink' or _is_asset_file(c)]
        if strict and not walked:
            raise ScanError(
                f"--strict asset directory is empty (fail-closed): "
                f"{matcher.redact_diagnostic(str(path))}"
            )
        candidates = sorted(walked, key=lambda t: str(t[0]))
    else:
        return issues

    for c, kind in candidates:
        rel = _rel_display(c)
        display = matcher.redact_path(rel)
        idx = matcher.path_hit_index(rel)
        if idx is not None:
            issues.append(Issue(path=display, offset=-1, pattern_index=idx, surface='filename'))

        if kind == 'symlink':
            try:
                target = os.readlink(c)
            except OSError as exc:
                if strict:
                    raise ScanError(f"cannot read asset symlink (fail-closed): {display}") from exc
                continue
            issues += matcher.scan(_encode_text(target), display)
            continue

        try:
            size = os.lstat(c).st_size
        except OSError as exc:
            if strict:
                raise ScanError(f"cannot stat asset file (fail-closed): {display}") from exc
            continue

        if _is_brotli(c.name):
            if size > _BR_COMPRESSED_CAP:
                raise ScanError("Brotli asset exceeds the compressed size cap (fail-closed)")
            data = _read_asset_bytes(c, display, strict)
            if data is None:
                continue
            issues += _scan_bytes_maybe_brotli(matcher, data, display, is_br=True)
            continue

        if size <= _WHOLE_READ_CAP:
            data = _read_asset_bytes(c, display, strict)
            if data is None:
                continue
            issues += matcher.scan(data, display)
        else:
            issues += _scan_streamed_file(matcher, c, display)

    return issues


def _read_asset_bytes(c: Path, display: str, strict: bool):
    try:
        return c.read_bytes()
    except OSError as exc:
        if strict:
            raise ScanError(f"cannot read asset file (fail-closed): {display}") from exc
        return None


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
        ("UTF-16LE no BOM", (pattern * 3).encode('utf-16-le')),
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

    # An empty/whitespace --scan-asset value is a hard usage error, never a
    # silently-absent option (HIGH-10): `--self-test --scan-asset ""` must NOT
    # slip through as a bare self-test.
    if args.scan_asset is not None and not args.scan_asset.strip():
        print("ERROR: --scan-asset requires a non-empty path.", file=sys.stderr)
        return 2

    asset_requested = args.scan_asset is not None

    # HIGH-10: presence is tested with `is not None` / the store_true flags --
    # NEVER truthiness of the option value.
    if args.self_test:
        if args.scan_repo or asset_requested or args.strict:
            print("ERROR: --self-test cannot be combined with --scan-repo / "
                  "--scan-asset / --strict.", file=sys.stderr)
            return 2
        return _run_self_test()

    if args.strict and not (args.scan_repo and asset_requested):
        print("ERROR: --strict requires BOTH --scan-repo and --scan-asset PATH.",
              file=sys.stderr)
        return 2

    if not args.scan_repo and not asset_requested:
        print("Nothing to do -- pass --scan-repo and/or --scan-asset PATH "
              "(or --self-test)", file=sys.stderr)
        return 2

    patterns = load_patterns()
    try:
        _require_patterns(patterns)
    except ScanError as exc:
        print(f"ERROR: {_sanitize(exc)}", file=sys.stderr)
        return 1

    try:
        issues: list[Issue] = []
        if args.scan_repo:
            issues += scan_repo(patterns)
        if asset_requested:
            issues += scan_asset(args.scan_asset, patterns, strict=args.strict)
    except ScanError as exc:
        print(f"ERROR (fail-closed): {_sanitize(exc)}", file=sys.stderr)
        return 1

    _report(issues)
    return 0 if not issues else 1


if __name__ == '__main__':
    sys.exit(main())
