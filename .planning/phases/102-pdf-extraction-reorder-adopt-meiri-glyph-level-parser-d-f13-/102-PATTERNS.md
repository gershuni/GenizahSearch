# Phase 102: LOCAL PDF Text-Layer Extraction Rewrite — Pattern Map

**Mapped:** 2026-05-29
**Files analyzed:** 6 (4 modified, 1 migration bump, 1 UI status surface)
**Analogs found:** 6 / 6

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `shared/local_indexer.py::extract_pdf_pages` (rewrite) | utility / transform | glyph-I/O → transform → yield | `ephraim_meiri_pdf_converter/pdf_to_docx.py::meiri_extract_page_text` (spike wrapper) | exact role-match |
| `shared/local_indexer.py::_rtl_line_classify` + de-space helpers (new) | utility | transform | `shared/local_indexer.py::_fix_sort_true_rtl_line` (Phase 101) | role-match (RTL gating pattern) |
| `shared/local_indexer.py::_write_page_doc` (D-06 diverge) | utility / CRUD | request-response | `shared/local_indexer.py::_write_page_doc` itself (current) | exact |
| `shared/local_indexer.py` — status wiring (D-08) | config / CRUD | — | `shared/local_indexer.py::_ERROR_STATUSES_KEPT` + folder counter SQL | exact |
| `shared/local_indexer_migrations.py` (version bump) | migration | CRUD | `shared/local_indexer_migrations.py::_migrate_1_to_2` | exact |
| `desktop/my_library_tab.py::_build_leaf_item_status` (D-08) | component / UI | request-response | same function, existing `encoding_error` branch | exact |

---

## Pattern Assignments

### `shared/local_indexer.py::extract_pdf_pages` — REWRITE (rawdict path)

**Current function being replaced** (lines 794–858):

```python
def extract_pdf_pages(
    filepath: str,
) -> Iterator[tuple[int, str, str]]:
    doc = fitz.open(filepath)
    try:
        title = (doc.metadata or {}).get("title") or os.path.basename(filepath)
        for page_num, page in enumerate(doc, start=1):
            # Primary: blocks mode preserves paragraph structure
            blocks = page.get_text("blocks")
            text_parts = [
                _collapse_intra_block_newlines(b[4])
                for b in blocks
                if b[6] == 0 and b[4].strip()
            ]
            text_parts = [p for p in text_parts if p]
            text = "\n\n".join(text_parts)

            # D-F4: detect pathological one-word-per-line, fall back to sort=True
            if _detect_single_word_per_line(text):
                try:
                    fallback_text = page.get_text("text", sort=True)
                    if fallback_text and fallback_text.strip():
                        text = _fix_sort_true_rtl_page(fallback_text)
                except Exception:
                    pass

            if len(text.strip()) < _EMPTY_PAGE_CHAR_THRESHOLD:
                continue
            yield page_num, text, title
    finally:
        doc.close()
```

**Analog — Meiri spike wrapper** (`ephraim_meiri_pdf_converter/pdf_to_docx.py` functions + `.planning/spikes/001-meiri-glyph-reorder-vs-current/compare_extractors.py:52-90`):

```python
def meiri_extract_page_text(page) -> str:
    d = page.get_text("rawdict")
    try:
        meiri._attach_nikud_page(d)
    except Exception:
        pass
    block_texts: list[str] = []
    for blk in d.get("blocks", []):
        if blk.get("type") != 0:
            continue
        lines = sorted(blk.get("lines", []), key=lambda ln: ln.get("bbox", ...)[1])
        line_texts: list[str] = []
        for ln in lines:
            spans = ln.get("spans", [])
            for sp in spans:
                meiri._normalize_span_dir(sp)
            # RTL line: rightmost span first
            ...
        block_texts.append("\n".join(line_texts))
    return "\n\n".join(t for t in block_texts if t.strip())
```

**Caller contract (UNCHANGED)** — `_extract_and_write_pdf` (lines 2495–2524) iterates `(page_num, text, title)`:

```python
for page_num, text, title in extract_pdf_pages(filepath):
    if cancel_check():
        self._rollback_partial(sys_id)
        return (pages_written, "cancelled", display_title)
    display_title = title
    total_chars += len(text)
    self._write_page_doc(
        sys_id, page_num, text, title, folder_id,
        chunk_locator=f"p. {page_num}",
    )
    pages_written += 1
if total_chars < _SCANNED_PDF_CHAR_THRESHOLD and pages_written == 0:
    return (0, "no_text_layer", display_title)
```

**New pipeline the rewrite must implement (D-01 through D-11):**

1. `page.get_text("rawdict", flags=TEXT_FLAGS_NO_IMAGES)` (D-01 + D-11)
2. `_attach_nikud_page(page_dict)` — Meiri `pdf_to_docx.py:791` — re-attach detached nikud spans BEFORE any glyph metric math
3. Per-line grouping from glyph y-bands (D-02), baseline/font-size-based (NOT fixed `y_tol=2.5`)
4. Classify each line RTL vs LTR by Hebrew ratio (adapt `_rtl_ratio` at line 311)
5. For RTL lines: de-space glyphs via **1.8× median gap** adaptive threshold (D-04), nikud excluded from gap math (D-06), producing word-unit bbox-unions (D-05 — NOT synthetic space glyphs)
6. For RTL lines: reorder segments via adapted `_normalize_span_dir` (Meiri `pdf_to_docx.py:691`) applied to word-unit groupings
7. Apply `_fix_visual_brackets` (Meiri `pdf_to_docx.py:653`) for F-C (RTL-gated)
8. D-03 LTR-damage guard: compare rawdict output against `blocks` fallback (token-count / Jaccard) per page; fall back to `blocks` for that page if rawdict loses
9. D-07 corrupt-encoding detection: codepoint-garbage ratio on the full-page text string
10. Emit `(page_num, display_text, title)` where `display_text` retains nikud (D-06)

---

### `shared/local_indexer.py` — RTL line classification + de-space helpers (new functions)

**Analog — `_fix_sort_true_rtl_line`** (lines 371–428) — the existing RTL-gating pattern to mirror:

```python
def _fix_sort_true_rtl_line(line: str) -> str:
    if _rtl_ratio(line) <= 0.4:
        return line                       # LTR pass-through gate
    tokens = line.split()
    if len(tokens) <= 1:
        return line
    runs: list[tuple[bool, list[str]]] = []
    current: list[str] = []
    current_is_rtl: bool | None = None
    for tok in tokens:
        tok_is_rtl = _rtl_ratio(tok) > 0.4
        if current_is_rtl is None or tok_is_rtl == current_is_rtl:
            current.append(tok)
            current_is_rtl = tok_is_rtl
        else:
            runs.append((current_is_rtl, current))
            current = [tok]
            current_is_rtl = tok_is_rtl
    if current:
        runs.append((current_is_rtl, current))
    return ' '.join(
        tok
        for is_rtl, run in reversed(runs)
        for tok in (list(reversed(run)) if is_rtl else run)
    )
```

**Key pattern to copy:** `if _rtl_ratio(line) <= 0.4: return line` — the LTR pass-through guard at the top of every RTL transform. The new per-line helpers MUST mirror this guard.

**`_rtl_ratio` definition** (line 311 — currently marked dead code, will become live):

```python
def _rtl_ratio(text: str) -> float:
    alpha = [c for c in text if c.isalpha()]
    if not alpha:
        return 0.0
    rtl = sum(1 for c in alpha if unicodedata.bidirectional(c) in ("R", "AL", "AN"))
    return rtl / len(alpha)
```

**Analog — `_collapse_intra_block_newlines`** (lines 458–468) — simple per-unit text transform pattern:

```python
def _collapse_intra_block_newlines(block_text: str) -> str:
    stripped = block_text.strip()
    if not stripped:
        return ''
    if '\n' not in stripped:
        return stripped
    return re.sub(r'\s*\n\s*', ' ', stripped)
```

**Analog for de-space — Spike README Finding 3 (not committed as code):**
The 1.8× median gap algorithm is described in `.planning/spikes/001-meiri-glyph-reorder-vs-current/README.md` lines 167–179:
- Collect center-x distances between consecutive glyphs on one line
- Drop nikud combining marks from the glyph list for metric computation
- Compute median inter-glyph gap
- Hard break if gap > 1.8× median; mid-gap break only when corroborated by explicit space glyph, punctuation boundary, font/span boundary, or abnormal-long-token (D-04 hysteresis)
- Group consecutive glyphs below threshold into word-unit bbox-unions

**Analog — Meiri `_regroup_lines`** (`pdf_to_docx.py:854–894`) — the line grouping pattern for D-02.
Note: D-02 mandates **baseline/font-size grouping**, not the fixed `y_tol=2.5` used by Meiri.
Meiri's pattern shows the merge-groups structure; the new code must compute tolerance dynamically:

```python
def _regroup_lines(blk: dict, y_tol: float = 2.5) -> list[dict]:
    raw = [ln for ln in blk.get("lines", []) if ln.get("spans")]
    if not raw:
        return []
    raw.sort(key=lambda ln: (ln["bbox"][1] + ln["bbox"][3]) / 2)
    groups: list[list[dict]] = []
    for ln in raw:
        yc = (ln["bbox"][1] + ln["bbox"][3]) / 2
        if groups:
            gyc = sum((l["bbox"][1] + l["bbox"][3]) / 2 for l in groups[-1]) / len(groups[-1])
            if abs(yc - gyc) <= y_tol:
                groups[-1].append(ln)
                continue
        groups.append([ln])
    ...
```

---

### `shared/local_indexer.py::_normalize_span_dir` (adapted from Meiri, RTL-gated)

**Analog — Meiri `_normalize_span_dir`** (`pdf_to_docx.py:691–784`) — the full RTL segment-reorder core to adapt:

```python
def _normalize_span_dir(span: dict) -> None:
    chars = span.get("chars")
    if not chars or len(chars) < 2:
        return
    # RTL gate: trigger on any Hebrew presence
    has_rtl = any(c.get("c") and 0x0590 <= ord(c["c"]) <= 0x07BF
                  for c in chars)
    if not has_rtl:
        return
    # Split into segments by direction reversal (x-jump up = new segment)
    MAX_BACKWARD_JUMP = 15.0
    segments: list[list[dict]] = [[chars[0]]]
    for i in range(1, len(chars)):
        prev_x = (chars[i - 1]["bbox"][0] + chars[i - 1]["bbox"][2]) / 2
        this_x = (chars[i]["bbox"][0] + chars[i]["bbox"][2]) / 2
        dx = this_x - prev_x
        if dx > 0 or dx < -MAX_BACKWARD_JUMP:
            segments.append([chars[i]])
        else:
            segments[-1].append(chars[i])
    if len(segments) == 1:
        return
    # Sort segments right-to-left; re-reverse embedded digit runs
    segments.sort(key=lambda seg: -max((c["bbox"][0] + c["bbox"][2]) / 2 for c in seg))
    # ... digit-run re-reversal + GAP_NEEDS_SPACE space insertion ...
    GAP_NEEDS_SPACE = 4.0
    span["chars"] = new_chars
    span["text"] = "".join(c.get("c", "") for c in new_chars)
```

**Phase 102 adaptation constraint (D-05 / Codex HIGH-3):** Do NOT pass word-unit groupings as synthetic zero-bbox space glyphs into `_normalize_span_dir`. Instead, build word-unit bbox-unions first (de-space step), then apply span direction normalization within each word unit. Zero-bbox synthetic spaces create bogus x-direction jumps inside `_normalize_span_dir`.

---

### `shared/local_indexer.py::_fix_visual_brackets` (adapted from Meiri, F-C)

**Analog — Meiri `_fix_visual_brackets`** (`pdf_to_docx.py:643–682`):

```python
# Bracket mirror tables (pdf_to_docx.py:643-650)
_BRACKET_PAIRS = [("(", ")"), ("[", "]"), ("{", "}")]
_MIRROR_OF = {o: c for o, c in _BRACKET_PAIRS}
_MIRROR_OF.update({c: o for o, c in _BRACKET_PAIRS})
_CLOSERS = {c for _, c in _BRACKET_PAIRS}
_BRACKETS = set(_MIRROR_OF.keys())

def _fix_visual_brackets(lines: list[dict]) -> None:
    sorted_lines = sorted(lines, key=lambda ln: ln["bbox"][1])
    brackets: list[dict] = []
    for ln in sorted_lines:
        line_chars = [c for sp in ln.get("spans", []) for c in sp.get("chars", [])
                      if c.get("c") in _BRACKETS]
        line_chars.sort(key=lambda c: -((c["bbox"][0] + c["bbox"][2]) / 2))
        brackets.extend(line_chars)
    i = 0
    while i + 1 < len(brackets):
        a, b = brackets[i], brackets[i + 1]
        a_ch, b_ch = a.get("c"), b.get("c")
        if a_ch in _CLOSERS and b_ch not in _CLOSERS and _MIRROR_OF[a_ch] == b_ch:
            a["c"] = _MIRROR_OF[a_ch]
            b["c"] = _MIRROR_OF[b_ch]
        i += 2
```

Copy `_BRACKET_PAIRS`, `_MIRROR_OF`, `_CLOSERS`, `_BRACKETS` verbatim. The function operates on `lines: list[dict]` (rawdict line objects from a page block) — gate application to RTL-classified lines only.

---

### `shared/local_indexer.py::_write_page_doc` — D-06 content/cached_text divergence

**Current function** (lines 2420–2493) — the load-bearing single write site. Both `content` and `cached_text` currently receive the same `text`:

```python
def _write_page_doc(
    self,
    sys_id: str,
    page_num: int,
    text: str,           # <- TODAY: same text for both content and cached_text
    title: str,
    folder_id: int,
    chunk_locator: str = "",
) -> str:
    ...
    doc = tantivy.Document(
        unique_id=[uid],
        content=[text],           # <- line 2466: Tantivy index field
        ...
    )
    self._writer.add_document(doc)

    cached_bytes, uncompressed_len = compress_cached_text(text)   # <- line 2484
    self._conn.execute(
        "INSERT OR REPLACE INTO local_pages "
        "(sys_id, uid, page_num, cached_text, cached_text_codec, "
        " cached_text_uncompressed_len, extraction_format_version, chunk_locator) "
        "VALUES (?, ?, ?, ?, 'zstd', ?, 1, ?)",
        (sys_id, uid, page_num, cached_bytes, uncompressed_len, chunk_locator or ""),
    )
```

**D-06 change:** The caller (`extract_pdf_pages`) yields `display_text` (with nikud). `_write_page_doc` (or its caller `_extract_and_write_pdf`) must produce two strings:
- `index_text = strip_nikud(display_text)` — written to `content=[index_text]`
- `cached_bytes = compress_cached_text(display_text)` — stores nikud-bearing text

**Reuse pattern for stripping** — `genizah_core.strip_nikud` (line 199):

```python
NIKUD_PATTERN = re.compile(r'[֑-׏]')   # genizah_core.py:157

def strip_nikud(text: str) -> str:
    if not text:
        return text
    return NIKUD_PATTERN.sub('', text)
```

Import: `from genizah_core import strip_nikud` (already importable — genizah_core is a top-level module).

**`extraction_format_version` bump:** The hardcoded `1` on line 2489 must become `2` (or the current constant) to flag pages extracted by the new rawdict pipeline. See migration section below.

---

### `shared/local_indexer.py` — D-08 `corrupt_encoding` status wiring (4 surfaces)

**Surface 1 — `_ERROR_STATUSES_KEPT`** (lines 125–130): Add `'corrupt_encoding'` to the set:

```python
_ERROR_STATUSES_KEPT = {
    "oversized", "error", "encoding_error",
    "changed_during_index", "zip_bomb_suspected",
    "unreachable", "timeout",
    # Phase 102 D-08: detect corrupt text-layer encoding (F-G)
    # "corrupt_encoding",   <-- add here
}
```

**Surface 2 — scan classification** (line 1951): The `if status in (...)` branch that increments `result["indexed"]` vs `result["errors"]`:

```python
# current (line 1951):
if status in ("ok", "no_text_layer", "encoding_error", "unsupported"):
    result["indexed"] += 1
else:
    result["errors"] += 1
```

Add `"corrupt_encoding"` to the error-counted group (it is a future-OCR candidate, not successfully indexed):

```python
if status in ("ok", "no_text_layer", "unsupported"):
    result["indexed"] += 1
# "encoding_error" and "corrupt_encoding" go to errors (unfixable without OCR)
else:
    result["errors"] += 1
```

**Surface 3 — folder counter aggregation SQL** (lines 2868–2900): The `error_count` subquery currently enumerates specific status codes. Add `'corrupt_encoding'`:

```python
error_count = (
    SELECT COUNT(*) FROM local_files
    WHERE local_files.folder_id = folders.folder_id
      AND local_files.extraction_status IN (
          'error', 'encoding_error', 'changed_during_index', 'no_text_layer'
          -- Phase 102: 'corrupt_encoding'
      )
),
```

**Surface 4 — tree label/color** — see `desktop/my_library_tab.py` section below.

Also: `_migrate_1_to_2` at `shared/local_indexer_migrations.py:47–58` lists `_KEPT_STATUSES` which filters which rows survive the D-NEW-4 prune — `'corrupt_encoding'` must appear there too (see migration section).

---

### `shared/local_indexer_migrations.py` — extraction_format_version bump + `corrupt_encoding` kept

**Current migration ladder** (lines 156–end): Runs steps 0→1→2 in sequence. Phase 102 adds step 2→3.

**Pattern — existing `_migrate_1_to_2`** (lines 85–146):

```python
def _migrate_1_to_2(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN cached_text BLOB")
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN extraction_format_version INTEGER NOT NULL DEFAULT 1")
    ...

_LATEST_VERSION = 2   # line 33 — bump to 3

_MIGRATIONS: dict[int, object] = {
    0: _migrate_0_to_1,
    1: _migrate_1_to_2,
    # Phase 102: 2: _migrate_2_to_3,
}
```

**`_migrate_2_to_3` pattern to implement:**
- `_KEPT_STATUSES` (lines 47–58): add `'corrupt_encoding'` to the tuple
- No new columns needed (existing `extraction_format_version` column carries value 2 going forward)
- The migration itself is a no-op DDL change — but bumps `user_version` to 3 so existing rows with `extraction_format_version=1` are identifiable as pre-Phase-102 for "Re-index All" recovery

**`_alter_safe` helper** (lines 61–68) — reuse verbatim for any new column additions:

```python
def _alter_safe(cur: sqlite3.Cursor, ddl: str) -> None:
    try:
        cur.execute(ddl)
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            pass
        else:
            raise
```

**`run()` function** (line 156) — transaction loop pattern. Each migration runs in its own BEGIN IMMEDIATE / COMMIT / ROLLBACK — copy this pattern exactly for step 2→3.

---

### `desktop/my_library_tab.py::_build_leaf_item_status` — D-08 tree label/color

**Current function** (lines 333–358) — add `corrupt_encoding` branch modeled on the existing `encoding_error` branch:

```python
def _build_leaf_item_status(self, prior_st: str, prior_pages: int) -> tuple:
    pages_str = str(prior_pages) if prior_pages and prior_pages > 0 else ''
    if prior_st == 'ok':
        return pages_str, tr("OK"), None
    if prior_st == 'cancelled':
        return pages_str, tr("Cancelled"), '#e67e22'
    if prior_st == 'no_text_layer':
        return pages_str, tr("No text layer"), None
    if prior_st == 'encoding_error':
        return pages_str, tr("Encoding error"), '#e74c3c'
    # Phase 102 D-08: add before the final fallback:
    # if prior_st == 'corrupt_encoding':
    #     return pages_str, tr("Corrupt encoding"), '#e74c3c'
    if prior_st == 'unsupported':
        return pages_str, tr("Unsupported"), None
    ...
    return pages_str, '', None
```

**Also wire `update_file_status`** (lines 476–529) — the live-update path that paints tree rows red/orange. The `elif status == "encoding_error"` branch at line 486 and the `if status in ('error', 'encoding_error')` color-paint at line 519 both need `'corrupt_encoding'` added:

```python
# line 486 pattern:
elif status == "encoding_error":
    display_status = tr("Encoding error")
# Phase 102: add after:
# elif status == "corrupt_encoding":
#     display_status = tr("Corrupt encoding")

# line 519 pattern:
if status in ('error', 'encoding_error'):
    for col in range(3):
        leaf.setForeground(col, QColor('#e74c3c'))
# Phase 102: extend to:
# if status in ('error', 'encoding_error', 'corrupt_encoding'):
```

---

## Shared Patterns

### RTL-gate: LTR pass-through guard
**Source:** `shared/local_indexer.py:371–401` (`_fix_sort_true_rtl_line`)
**Apply to:** ALL new RTL-transform helpers (de-space, reorder, bracket fix)

```python
if _rtl_ratio(line_or_text) <= 0.4:
    return line_or_text   # LTR/numeric/empty — no transform
```

### Glyph center-x computation
**Source:** `ephraim_meiri_pdf_converter/pdf_to_docx.py:727,741,773`
**Apply to:** de-space gap computation, segment sorting, gap measurement

```python
x_center = (char["bbox"][0] + char["bbox"][2]) / 2
```

### Hebrew range constants
**Source:** `ephraim_meiri_pdf_converter/pdf_to_docx.py:708` and `compare_extractors.py:45–49`

```python
# Full Hebrew block including nikud (for RTL gate):
has_rtl = any(c.get("c") and 0x0590 <= ord(c["c"]) <= 0x07BF for c in chars)
# Hebrew letters only (for word-unit identification):
_is_hebrew_letter = bool(ch) and 0x05D0 <= ord(ch) <= 0x05EA
# Nikud combining marks (exclude from gap math — D-06):
_is_nikud_cp = 0x05B0 <= cp <= 0x05C7   # pdf_to_docx.py:787-788
```

### Nikud stripping for index field
**Source:** `genizah_core.py:157,199–206`
**Apply to:** `_write_page_doc` content field (D-06)

```python
NIKUD_PATTERN = re.compile(r'[֑-׏]')
index_text = NIKUD_PATTERN.sub('', display_text)   # or: strip_nikud(display_text)
```

### zstd cached_text write (unchanged)
**Source:** `shared/local_indexer.py:635–644`, `2484–2491`
**Apply to:** `_write_page_doc` cached_text write — keep exactly, just pass `display_text` (nikud-bearing)

```python
cached_bytes, uncompressed_len = compress_cached_text(display_text)
```

### Error status propagation (4 surfaces)
**Source:** `shared/local_indexer.py:126–130` (`_ERROR_STATUSES_KEPT`), `1951–1954` (scan classification), `2868–2900` (folder counter SQL), `desktop/my_library_tab.py:333–358` (`_build_leaf_item_status`)
**Apply to:** `corrupt_encoding` — must appear in ALL 4 surfaces or it counts/displays wrong (Codex HIGH-4)

---

## No Analog Found

No files are truly analog-free — all have direct matches. The de-space algorithm (D-04) has no committed code analog; the planner must derive it from the spike README description (`.planning/spikes/001-meiri-glyph-reorder-vs-current/README.md:167–179`).

| Missing piece | Role | Data Flow | Reason |
|---------------|------|-----------|--------|
| Adaptive 1.8× median de-space function | utility / transform | glyph-I/O → transform | Prototyped in spike README, not committed as code; re-derive from description |
| D-07 corrupt-encoding detector | utility | transform | No existing codepoint-garbage scanner in the codebase |
| D-09 multi-column suspect detector | utility | transform | No column-layout detector exists; cheap heuristic to implement from scratch |

---

## Line Number Verification

All line numbers confirmed against live files (2026-05-29):

| Reference | Confirmed location |
|-----------|-------------------|
| `_ERROR_STATUSES_KEPT` | `shared/local_indexer.py:126` |
| `_fix_sort_true_rtl_line` | `shared/local_indexer.py:371` |
| `_fix_sort_true_rtl_page` | `shared/local_indexer.py:431` |
| `_collapse_intra_block_newlines` | `shared/local_indexer.py:458` |
| `_detect_single_word_per_line` | `shared/local_indexer.py:483` |
| `_rtl_ratio` | `shared/local_indexer.py:311` (currently dead code) |
| `compress_cached_text` | `shared/local_indexer.py:635` |
| `extract_pdf_pages` | `shared/local_indexer.py:794` |
| `_write_page_doc` | `shared/local_indexer.py:2420` |
| Tantivy `content=[text]` | `shared/local_indexer.py:2466` |
| `compress_cached_text(text)` call | `shared/local_indexer.py:2484` |
| `_extract_and_write_pdf` | `shared/local_indexer.py:2495` |
| scan classification | `shared/local_indexer.py:1951` |
| folder counter SQL | `shared/local_indexer.py:2868` |
| `_normalize_span_dir` | `ephraim_meiri_pdf_converter/pdf_to_docx.py:691` |
| `_fix_visual_brackets` | `ephraim_meiri_pdf_converter/pdf_to_docx.py:653` |
| `_attach_nikud_page` | `ephraim_meiri_pdf_converter/pdf_to_docx.py:791` |
| `_regroup_lines` | `ephraim_meiri_pdf_converter/pdf_to_docx.py:854` |
| `_span_text` | `ephraim_meiri_pdf_converter/pdf_to_docx.py:897` |
| `strip_nikud` | `genizah_core.py:199` |
| `NIKUD_PATTERN` | `genizah_core.py:157` |
| `strip_search_diacritics` | `genizah_core.py:6302` |
| `_build_leaf_item_status` | `desktop/my_library_tab.py:333` |
| `update_file_status` encoding branch | `desktop/my_library_tab.py:486` |
| `update_file_status` color paint | `desktop/my_library_tab.py:519` |
| `_migrate_1_to_2` | `shared/local_indexer_migrations.py:85` |
| `_KEPT_STATUSES` | `shared/local_indexer_migrations.py:47` |
| `_LATEST_VERSION` | `shared/local_indexer_migrations.py:33` |

**Note on CONTEXT.md line numbers:** All cited lines matched the live files exactly. The `_rtl_ratio` function cited as dead code at line 311 will become live in Phase 102.

## Metadata

**Analog search scope:** `shared/`, `ephraim_meiri_pdf_converter/`, `desktop/`, `genizah_core.py`, `shared/local_indexer_migrations.py`
**Files scanned:** 7 source files + spike directory
**Pattern extraction date:** 2026-05-29
