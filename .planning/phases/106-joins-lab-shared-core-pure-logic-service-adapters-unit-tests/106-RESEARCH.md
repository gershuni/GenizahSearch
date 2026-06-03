# Phase 106: Joins Lab Shared Core — Research

**Researched:** 2026-06-03
**Domain:** Pure domain logic extraction + service adapter design for the Joins Lab (Python, shared module layer)
**Confidence:** HIGH (all critical findings verified against live source code)

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** New shared module is a **single file `shared/joins_lab.py`** — all six logic units, the `SearchExecutor` protocol, and the dataclasses live in one module. Matches the existing `shared/*_service.py` single-file convention; no package, no submodule split, no `__init__` barrel.
- **D-02:** The domain model is **typed**: frozen dataclasses for `BuilderRow`, `SideQuery`, `Candidate` (explicit provenance fields + a canonical candidate key), and the merge result. The shared module owns the **single `dict → Candidate` normalizer** (one source of truth).
- **D-03:** `SearchExecutor` is a **narrow Protocol over the search engine only**: `execute_search(...)`, `get_browse_page(...)`, `get_meta_for_id(...)`, `get_library_for_id(...)`. It is the single injected runtime dependency; only **cross-side membership** needs it live.
- **D-04:** The adapter **returns the engine's raw result dicts**; the shared module normalizes them to `Candidate`. Each app's implementation stays a **thin passthrough**.
- **D-05:** VS look-alikes and material/measurement enrichment are **NOT in the adapter** — reached via `shared/visual_similarity_service.py` and `shared/fjms_service.py`. No direct `fist_data/*.db`.
- **D-06:** Pure functions take **already-fetched data** — no adapter, no I/O. A `FakeSearchExecutor` covers the one I/O-bound unit (cross-side membership).
- **D-07:** Builder input model = `{term, line_start: bool, line_end: bool, gap_to_next: int}` per row, plus a per-`SideQuery` global `variants` toggle. Composition emits engine's line-break syntax.
- **D-08:** Page-level anchors: **page-START anchor ONLY on first row**, **page-END ONLY on last row**. Page anchor is **independent of** per-row line-START/END.
- **D-09:** Dataclasses are additive-extensible (fields with defaults). No speculative fields.
- **D-10:** Editable raw composed-query preview = DEFERRED. `compose()` is one-way.
- **D-11:** Known-joins grouping (BFS) is NOT in 106 — Phase 107.
- **D-12:** All JSA / parallels logic is OUT of 106 — Phase 110.
- **D-13:** "Other side" = adjacent image `p±1` within the same sys_id. Multi-leaf adjacency = deferred.

### Claude's Discretion

- Exact dataclass field names and module-internal helper decomposition.
- `FakeSearchExecutor` / fixture design; whether round-trip tests import the pure module-level `genizah_core._parse_line_break_query` directly vs a stub.
- Snippet centering parameters (`max_lines`/`max_chars`) and highlight MARK-token internals.

### Deferred Ideas (OUT OF SCOPE)

- Per-row variation columns (per-term variants/fuzzy)
- Editable raw composed-query preview (string↔rows round-trip)
- Richer N-fragment / per-edge evidence+confidence join model
- JSA-02 (corpus-frequency completion) and JSA-03 (`[`/`]`-aware torn-word completion)
- Multi-leaf / bifolio "other side" adjacency beyond `p±1`
- Web Joins Lab UI
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| JWB-10 (foundational logic) | Line-by-line query builder: per-row line START/END anchors + gap → engine line-break syntax (`\|` groups, `[|N]` gaps). RTL: START anchor on the right. | SC#1. `compose()` from sketch L560; round-trips against `genizah_core._parse_line_break_query` (L5811). |
| JWB-11 (foundational logic) | Cross-side AND/OR: "other side" = adjacent image `p±1`. AND = post-filter; OR = union. Decided by `(sys_id, page±1)` set membership. | SC#2. `_CrossSideWorker.run` sketch L387; `get_browse_page` for `total_pages`. |
| JWB-12 (foundational logic) | Unified candidates: text / VS / combined. Provenance badges (★both / ⊙VS / ⇄other / ⚓self). Dedup one-per-image. Self-match detection + snippet/page helpers. | SC#3/4/5. `_on_results` L1102, `_maybe_assemble` L1149, `_anchor_matched` L1100. |
| Build constraints (architecture) | No PyQt, no direct `fist_data/*.db`, all data via shared services or `SearchExecutor`. Static import test. | SC#6. Verified: `_parse_line_break_query` importable standalone (confirmed via headless import test). |
</phase_requirements>

---

## Summary

Phase 106 extracts the validated Joins Lab domain logic from the throwaway sketch (`join_workbench.py.txt`) into a production-grade `shared/joins_lab.py` module. The sketch already contains all six logic units in PyQt-free form — the QThread wrappers strip away, leaving pure functions. The primary deliverable is a typed, tested module with a `SearchExecutor` Protocol boundary, not a new capability.

The two research flags (R-01 and R-02) are resolved with concrete code evidence below. The most important finding: **page-level anchoring (`content_head`/`content_tail`) and line-break syntax (`|` groups) run through the SAME engine dispatch path and CAN compose into a single `execute_search` call.** The `text_position` parameter is passed through to `_execute_line_break_search` and handled as a post-Tantivy position filter there. R-02: bracket stripping in the engine is controlled by `_query_has_brackets(query_str)` / `_strip_brackets(text)` — both are module-level pure functions that `shared/joins_lab.py` can import directly.

**Primary recommendation:** Plan five sequential tasks: (1) typed domain model + `SearchExecutor` Protocol + `dict→Candidate` normalizer; (2) `compose()` pure function + round-trip tests; (3) cross-side membership function + `FakeSearchExecutor` tests; (4) dedup/merge/provenance functions + tests; (5) self-match + snippet/page helpers + tests + static import guard.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Query composition (line-break syntax) | Shared module (`shared/joins_lab.py`) | — | Pure logic, no I/O; must be web-usable |
| Engine execution (candidates fetch) | API / Backend (`SearchEngine.execute_search`) | Via `SearchExecutor` adapter | Engine owns Tantivy index; adapter decouples the shared module from desktop/web wiring |
| Cross-side page membership | Shared module | Engine (via `SearchExecutor.get_browse_page`) | Set logic is pure; total_pages comes from the engine |
| Candidate dedup / compaction | Shared module | — | Pure; keyed on `(sys_id, page, uid)` |
| Text/VS merge ordering + provenance | Shared module | — | Pure; VS and text result sets are already-fetched input |
| Self-match detection | Shared module | — | Pure; checks anchor's sys_id against result dict set |
| Snippet/page helpers | Shared module | — | Pure text manipulation; regex highlighting |
| VS look-alikes fetch | `shared/visual_similarity_service.py` | — | Pre-existing service; Phase 105+109 wire it; Phase 106 only calls `get_suggestions` |
| Material/measurement fetch | `shared/fjms_service.py` | — | Pre-existing service; Phase 106 only calls `get_measurements` |
| Desktop `SearchExecutor` impl | Desktop layer (Phase 107+) | — | Thin passthrough wrapping `SearchEngine` + `MetadataManager` |

---

## R-01 Resolution (CRITICAL): Page-Anchor + Line-Break = One Call or Two?

**Answer: ONE `execute_search` call. The `text_position` parameter is passed through to `_execute_line_break_search` and handled there as a post-Tantivy position filter.**

### Code Trace

`execute_search` at `genizah_core.py:8298` receives `text_position`. When the query string contains line-break syntax (`|` separators or `[|N]` gaps), the path at lines 8356-8365 fires:

```python
# genizah_core.py:8356-8365
line_groups, line_gaps = _parse_line_break_query(query_str)
if line_groups is not None:
    return self._execute_line_break_search(
        line_groups, line_gaps, query_str,
        responsa_options=responsa_options,
        ...
        text_position=text_position,        # <-- forwarded
    )
```

`_execute_line_break_search` at lines 8001-8219 uses `text_position` as a post-regex position filter at lines 8142-8153:

```python
# genizah_core.py:8142-8153
if text_position == 'start' and match_obj.start() > 0:
    prefix = content[:match_obj.start()]
    cleaned = prefix.strip() if _brackets_in_query else _strip_brackets(prefix).strip()
    if cleaned:
        regex_filtered += 1
        continue
elif text_position == 'end' and match_obj.end() < len(content):
    suffix = content[match_obj.end():]
    cleaned = suffix.strip() if _brackets_in_query else _strip_brackets(suffix).strip()
    if cleaned:
        regex_filtered += 1
        continue
```

**Concrete interpretation for D-08:**
- A first-row page-START anchor composes into: `execute_search(query_str_with_pipe_syntax, ..., text_position='start')`
- A last-row page-END anchor composes into: `execute_search(query_str_with_pipe_syntax, ..., text_position='end')`
- `text_position='start'` means "match must have nothing before it in the document" (post-filter in `_execute_line_break_search`)
- `text_position='end'` means "match must have nothing after it in the document"

**Implication for `compose()` and the adapter contract:**
- `SideQuery` must carry an optional `page_position: str | None` field (`'start'`, `'end'`, or `None`).
- `compose()` returns `(query_str, responsa_options, page_position)` — the caller passes `page_position` as `text_position` to `execute_search`.
- No intersection step needed. The `content_head`/`content_tail` Tantivy field path (used by non-line-break queries at line 8570) is NOT used by the line-break path. The line-break path uses `content` for Tantivy and position-checks post-regex.
- SC#1 round-trip MUST include a case where `page_position='start'` is set on the first row; the test verifies that `_execute_line_break_search` receives `text_position='start'`.

**Note on the two Tantivy paths for `text_position`:**
The non-line-break path (lines 8564-8580) maps `text_position` to `content_head`/`content_tail` Tantivy fields. The line-break path ignores this mapping and uses `content` for Tantivy, then applies position as a post-filter. These are cleanly separate; the shared module only ever composes line-break queries so it always goes through the post-filter path.

---

## R-02 Resolution: Leading Tear-Bracket Tokens and the `line_start` Test

**Answer: The engine strips brackets from content before applying the line-start/line-end regex position check ONLY when the query itself contains no literal brackets. The bracket stripping is controlled by `_query_has_brackets(query_str)` → `_strip_brackets(content)` at lines 8124-8128 and 8142-8153. For a typical scholar query (no literal `[`/`]` in the query), the engine strips brackets from content before matching.**

### Code Trace

In `_execute_line_break_search` at lines 8124-8128:

```python
# genizah_core.py:8124-8128
_brackets_in_query = _query_has_brackets(query_str)
...
match_content = content if _brackets_in_query else _strip_brackets(content)
match_obj = regex.search(match_content)
```

`_query_has_brackets` (line 6342) strips gap tokens (`[3]`, `[|2]`) before checking, so a line-break query like `|שהדותא [|1] ממשלה|` has no literal brackets — the function returns `False`, and `match_content = _strip_brackets(content)`. 

`_strip_brackets` (line 6352) removes all `[` and `]` from the content text. So a corpus line `]שהדותא ממשלה` becomes `שהדותא ממשלה` before the regex match. A line_start query for `שהדותא` will match the stripped line at position 0, satisfying the line-start constraint.

The page-level position check at lines 8142-8153 similarly strips brackets from the prefix/suffix check when the query is bracket-free.

**Implication for self-match detection (SC#5):**
Self-match uses the same `execute_search` path, so if the anchor's own text starts with `]`, the bracket is stripped before the line-start test — the self-match check naturally handles this correctly as long as it uses the same engine call rather than a hand-rolled regex test.

**Implication for `compose()` (D-07/D-08):**
The scholar's query terms must NOT themselves contain `[` or `]` (user types the clean Hebrew word, not the tear-marked form). If the query is bracket-free, the engine's bracket-stripping handles the corpus side transparently. This is a constraint the `BuilderRow.term` field should document: terms are the search tokens, not raw corpus text.

**Module-level functions (importable without heavy init):**

```python
# genizah_core.py:6342
def _query_has_brackets(query_str: str) -> bool: ...

# genizah_core.py:6352  
def _strip_brackets(text: str) -> str: ...
```

Both are module-level pure functions. `shared/joins_lab.py` can import them directly for any bracket-handling logic it needs (e.g., in snippet centering or self-match display).

---

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Python dataclasses | stdlib | `BuilderRow`, `SideQuery`, `Candidate`, merge result | Already used across `shared/` (e.g., `shared/puzzle_model.py`) |
| `typing.Protocol` | stdlib (3.8+) | `SearchExecutor` structural subtyping | No runtime dependency; web + desktop can implement independently |
| `re` | stdlib | Snippet centering, bracket detection | Already used in sketch helpers |
| `pytest` | existing | Unit test framework | All 2500+ project tests use pytest |

### Supporting (consumed via existing shared services — NOT added as deps)
| Service | File | Method | Purpose |
|---------|------|--------|---------|
| Visual Similarity | `shared/visual_similarity_service.py` | `get_vs_service().get_suggestions(sys_id, limit)` | VS look-alikes for candidate merge |
| FJMS measurements | `shared/fjms_service.py` | `get_fjms_service().get_measurements(sys_id)` | Material + dimensions per candidate |

**Installation:** No new dependencies. `shared/joins_lab.py` uses only stdlib + the existing shared services.

---

## Primitive Inventory: Six Logic Units

### Unit 1 — Line-Break Query Composition (SC#1)

**Sketch behavior:** `QueryBuilder.compose()` (~L560). Iterates builder rows; for each non-empty row: marks first word with leading `|` if `line_start` is True (so the engine parser marks `current_line_start=True`); marks last word with trailing `|` if `line_end` is True; inserts `[|N]` gap tokens between rows where `gap_to_next > 0` (or `[|0]` for gap=0 / consecutive). Returns `(query_str, responsa_options)`.

**Sketch compose() verbatim logic (pure, no PyQt):**
```python
# From sketch L560-581
def compose(rows, variants: bool) -> tuple[str | None, dict | None]:
    rows = [r for r in rows if r.term.strip()]
    if not rows:
        return None, None
    ro = {"responsa_mode": True, "variants": variants, "ja": False,
          "flex_spacing": False, "bidirectional": False,
          "variant_mode": "variants" if variants else "exact"}
    multiline = len(rows) > 1 or any(r.line_start or r.line_end for r in rows)
    if not multiline:
        return rows[0].term.strip(), ro
    parts = []
    for i, row in enumerate(rows):
        toks = row.term.strip().split()
        if not toks:
            continue
        if row.line_start:
            toks[0] = "|" + toks[0]
        if row.line_end:
            toks[-1] = toks[-1] + "|"
        parts.append(" ".join(toks))
        if i < len(rows) - 1:
            parts.append(f"[|{row.gap_to_next}]")
    return " ".join(parts), ro
```

**Split:** Pure function. `compose(rows: list[BuilderRow], variants: bool) -> tuple[str | None, dict | None]`. No I/O.

**Engine dependency verified:**
- `genizah_core._parse_line_break_query` at `genizah_core.py:5811` — module-level pure function.
- **Import test confirmed:** `from genizah_core import _parse_line_break_query` imports without `MetadataManager`, `VariantManager`, or `SearchEngine` initialization (verified via headless `python -c "import genizah_core; from genizah_core import _parse_line_break_query"` — output: `imported OK`). [VERIFIED: headless Python import]
- Round-trip: `compose(rows)` → `_parse_line_break_query(query_str)` → `(line_groups, line_gaps)` where `line_groups[i].line_start` / `line_groups[i].line_end` match the input rows.

**Page anchor (D-08):**
- `SideQuery` carries `page_position: str | None = None` (values: `'start'`, `'end'`, `None`).
- `page_position='start'` is only valid when the first row has content; `page_position='end'` only when the last row has content. The shared module enforces this constraint.
- The adapter passes `page_position` as `text_position` to `execute_search`. The engine handles it in `_execute_line_break_search` as a post-regex filter (R-01 resolution above).

### Unit 2 — Cross-Side AND/OR Membership (SC#2)

**Sketch behavior:** `_CrossSideWorker.run()` (~L387-429). Runs query B through the engine, builds `bset = set of (sid, page)`. For **AND**: keeps only base candidates where `(sid, p-1) in bset or (sid, p+1) in bset`. For **OR**: keeps all base candidates PLUS synthesizes neighbor result-dicts for each `(sid, q)` in `bset` where `q-1` or `q+1` is not already in the base set.

**Split:** I/O-bound (requires `execute_search` for query B). Wrapped in a function that takes a `SearchExecutor`, a list of base result dicts, query-B string + responsa_options, and a combine mode.

**Pure sub-function:**
- `resolve_other_side_pages(p: int, total_pages: int) -> frozenset[int]` — given anchor page and total, returns `{p+1}` if first, `{p-1}` if last, `{p-1, p+1}` if middle. Pure; no I/O.
- `cross_side_membership(base_set: set[tuple], b_set: set[tuple], combine: str) -> set[tuple]` — pure AND/OR set logic.

**`get_browse_page` signature verified:**
```python
# genizah_core.py:9483
def get_browse_page(self, sys_id, p_num=None, next_prev=0, absolute_index=None, allow_cross=False, volume_ie=None)
# Returns: {'uid', 'p_num', 'full_header', 'text', 'total_pages', 'current_idx', 'internal_index', 'sys_id', 'volume_ie'}
# Returns None when page not found or at boundary.
```
The `total_pages` key is needed for `resolve_other_side_pages`. The `SearchExecutor.get_browse_page(sys_id, p_num)` Protocol method wraps this.

### Unit 3 — Candidate Dedup / Compaction (SC#3)

**Sketch behavior:** `_on_results` dedup block (~L1102-1114). Iterates raw results; builds a canonical key per result; skips duplicates; marks anchor-self hits.

**Canonical key from sketch:**
```python
key = r.get("uid") or f"{r_sid(r)}|{(r.get('display') or {}).get('img')}"
```

**Production canonical key (D-02):** `(sys_id, page)` tuple where `sys_id = display.id` and `page = display.img` (or parsed from `uid`). The `page_of(res)` helper (sketch L84-95) is the single source for `page`. The full `uid` is included for VS-sourced candidates that may have `uid="{sid}|vs"`.

**Split:** Pure function. `dedup_candidates(raw: list[dict], anchor_sid: str, include_self: bool) -> tuple[list[Candidate], bool]` (returns deduped list + `anchor_matched` flag).

### Unit 4 — Text/VS Merge Ordering with Provenance (SC#4)

**Sketch behavior:** `_maybe_assemble` (~L1149-1174). Annotates text candidates that also appear in the VS set with `_via_vs=True` and `vs_rank`. Appends VS-only candidates. Sorts by: tier 0 = both (`_via_text AND _via_vs`) → tier 1 = text-only → tier 2 = VS-only; within VS tier, by `vs_rank`.

**Provenance fields on result dicts (from sketch):**
- `_via_text: bool` — set by the dedup step
- `_via_vs: bool` — set by the merge step
- `_is_anchor_self: bool` — set by dedup
- `_via_other_side: bool` — set by cross-side OR path
- `vs_rank: int | None` — from VS service

**Split:** Pure function. `merge_candidates(text_cands: list[Candidate], vs_cands: list[Candidate]) -> list[Candidate]`. Takes already-fetched lists.

### Unit 5 — Self-Match Detection + Snippet/Page Helpers (SC#5)

**Self-match (sketch L1100):**
```python
self._anchor_matched = any(r_sid(r) == self.anchor_sid for r in results)
```
Pure: `detect_self_match(raw_results: list[dict], anchor_sid: str) -> bool`.

**Page helper (sketch L84-95) — pure:**
```python
def page_of(res) -> int | None:
    p = _to_int((res.get("display") or {}).get("img"))
    if p: return p
    m = re.search(r"_P0*(\d+)", res.get("uid") or "")
    return int(m.group(1)) if m else None
```

**Snippet helpers (sketch L126-148) — pure:**
```python
def snippet_html(text, pattern, max_lines=8) -> str:
    # Center on first regex match; HTMLify with RTL wrapper

def snippet_plain(text, pattern, max_chars=220) -> str:
    # Center on first match; plain text for table cells
```

**`_match_line` (sketch L113-123) — pure:**
```python
def _match_line(lines, pattern) -> int:
    # Returns index of first matching line, -1 if no match
```

### Unit 6 — Static Import Guard (SC#6)

**Behavior:** A test that imports `shared.joins_lab` and asserts:
1. No `PyQt6` / `PyQt5` / `PySide6` symbol in the module's namespace or imports.
2. No `sqlite3.connect` call to a path containing `fist_data` (AST inspection or grep).

**Verified importability:** `genizah_core._parse_line_break_query` is module-level and imports cleanly without engine init (confirmed above). `shared/joins_lab.py` will import only stdlib + `genizah_core._parse_line_break_query` + `genizah_core._query_has_brackets` + `genizah_core._strip_brackets` (all module-level) + the `SearchExecutor` Protocol (no instantiation). [VERIFIED: headless import test]

---

## SearchExecutor Protocol — Exact Signature

Based on `DESKTOP-INTEGRATION-NOTES.md` (verified against live code) [VERIFIED: genizah_core.py]:

```python
from typing import Protocol, Any

class SearchExecutor(Protocol):
    def execute_search(
        self,
        query_str: str,
        mode: str,
        gap: int,
        progress_callback=None,
        exclude_words=None,
        responsa_options: dict | None = None,
        restrict_sys_ids: set | None = None,
        text_position: str | None = None,
        corpus_scope: str = "genizah",
    ) -> list[dict]: ...

    def get_browse_page(
        self,
        sys_id: str,
        p_num: int | None = None,
        next_prev: int = 0,
        absolute_index: int | None = None,
        allow_cross: bool = False,
        volume_ie: str | None = None,
    ) -> dict | None: ...
    # Returns: {'uid', 'p_num', 'full_header', 'text', 'total_pages',
    #           'current_idx', 'internal_index', 'sys_id', 'volume_ie'}
    # Returns None at boundaries.

    def get_meta_for_id(self, sys_id: str) -> tuple[str, str]: ...
    # Returns: (shelfmark, title)
    # Source: genizah_core.py:3695 MetadataManager.get_meta_for_id

    def get_library_for_id(self, sys_id: str) -> str: ...
    # Returns: library_code string (e.g. 'CUL', 'JTS') or ''
    # Source: genizah_core.py:3731 MetadataManager.get_library_for_id
```

`execute_search` and `get_browse_page` live on `SearchEngine`; `get_meta_for_id` and `get_library_for_id` live on `MetadataManager`. The desktop `SearchExecutor` implementation wraps both. This is exactly the split the sketch used. [VERIFIED: genizah_core.py:8298, 9483, 3695, 3731]

---

## Result Dict Shape (for `dict → Candidate` Normalizer)

From `DESKTOP-INTEGRATION-NOTES.md` and `_execute_line_break_search` output (lines 8180-8200): [VERIFIED: genizah_core.py]

```
{
  'display': {
      'id': str,          # sys_id (AlmaId — long 99000... format)
      'shelfmark': str,
      'title': str,
      'library_code': str,
      'img': int | str,   # 1-based page/image number
      'source': str,
  },
  'full_text': str,
  'snippet': str,          # highlighted text fragment
  'uid': str,              # canonical identifier, format "{sys_id}_{source}_P{page}"
  'raw_header': str,
  'raw_file_hl': str,
  'highlight_pattern': str | None,  # regex pattern string for client-side re-highlight
  'score': float | None,
  'scope': str,            # 'page' | 'system'
  # Optional — from cross-side OR path:
  '_via_other_side': bool,
  # Optional — from VS merge:
  '_via_vs': bool,
  'svm_score': float | None,
  'vs_rank': int | None,
  # Optional — from dedup:
  '_via_text': bool,
  '_is_anchor_self': bool,
}
```

**Key accessors the normalizer needs:**
- `sys_id`: `(res.get("display") or {}).get("id") or res.get("sys_id") or ""`
- `page`: `page_of(res)` (see Unit 5 above) — handles both `display.img` and `_P{N}` uid patterns
- `shelfmark`: `(res.get("display") or {}).get("shelfmark") or res.get("uid") or "?"`
- `title`: `(res.get("display") or {}).get("title") or ""`
- `library_code`: `(res.get("display") or {}).get("library_code") or ""`

**VS-sourced result dicts** (from `_VsLoadWorker`) use `uid="{sid}|vs"` and have no `display.img` — `page_of` falls back to `None` for these, so the dedup key for VS-only is `(sys_id, None)`. When a VS candidate is also a text match, the text match's page wins (it has real `display.img`).

---

## Architecture Patterns

### Recommended Project Structure

```
shared/
├── joins_lab.py      # new — all six logic units + Protocol + dataclasses
tests/
├── test_joins_lab.py # new — class-based, all six SC# covered
```

No submodule, no package. Matches `shared/puzzle_model.py`, `shared/visual_similarity_service.py`.

### Pattern 1: Protocol over Structural Subtyping (SearchExecutor)

```python
# Source: CONTEXT.md D-03 + DESKTOP-INTEGRATION-NOTES.md (verified)
from typing import Protocol, runtime_checkable

@runtime_checkable
class SearchExecutor(Protocol):
    def execute_search(self, query_str, mode, gap, ...) -> list[dict]: ...
    def get_browse_page(self, sys_id, p_num=None, ...) -> dict | None: ...
    def get_meta_for_id(self, sys_id) -> tuple[str, str]: ...
    def get_library_for_id(self, sys_id) -> str: ...
```

The desktop `SearchExecutor` in Phase 107 will be a thin class wrapping `self.searcher` (SearchEngine) and `self.meta_mgr` (MetadataManager).

### Pattern 2: Frozen Dataclasses (Domain Model)

```python
# Source: CONTEXT.md D-02, sketch compose() L560
from dataclasses import dataclass, field

@dataclass(frozen=True)
class BuilderRow:
    term: str
    line_start: bool = False
    line_end: bool = False
    gap_to_next: int = 0

@dataclass(frozen=True)
class SideQuery:
    rows: tuple[BuilderRow, ...]
    variants: bool = False
    page_position: str | None = None   # 'start' | 'end' | None (D-08)
```

### Pattern 3: Pure Functions Take Already-Fetched Data (D-06)

```python
# Source: CONTEXT.md D-06; sketch structure
def compose(side_query: SideQuery) -> tuple[str | None, dict | None, str | None]:
    """Returns (query_str, responsa_options, page_position). Pure, no I/O."""
    ...

def merge_candidates(
    text_cands: list[Candidate],
    vs_cands: list[Candidate],
) -> list[Candidate]:
    """Pure merge. Caller already fetched both lists."""
    ...
```

### Anti-Patterns to Avoid

- **Direct sqlite3.connect in shared/joins_lab.py:** Violates D-05. Use `get_vs_service()` and `get_fjms_service()`.
- **Any PyQt6/PyQt5 import:** Violates SC#6. The module must be web-importable.
- **Embedding `execute_search` logic in compose():** compose() is pure; execution is the adapter's job.
- **Re-implementing `page_of` inline in multiple places:** One pure helper; normalizer calls it.
- **Using `display['img']` directly without `page_of`:** VS-sourced results use `uid="{sid}|vs"` — `display.img` may be missing or `1` (placeholder). Always use `page_of(res)`.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Line-break query parsing | Custom parser | `genizah_core._parse_line_break_query` | Already handles `|`, `[|N]`, leading/trailing `|` on tokens, modifiers; round-trip tested |
| Bracket stripping | Custom `replace('[','')` | `genizah_core._strip_brackets` | The project's canonical function; consistent with how the engine strips for match testing |
| Query-has-brackets check | Custom regex | `genizah_core._query_has_brackets` | Correctly excludes `[3]` / `[|2]` gap tokens from the check |
| VS look-alike fetch | Direct sqlite3 query on `fist_data/visual_similarity.db` | `shared/visual_similarity_service.get_vs_service().get_suggestions(sys_id, limit)` | Returns `list[{'alma_id', 'svm_score', 'rank'}]`; handles missing DB gracefully |
| Material/measurement fetch | Direct sqlite3 query | `shared/fjms_service.get_fjms_service().get_measurements(sys_id)` | Returns `{'summary': {...}, 'catalog_sizes': [...], ...}`; handles schema drift |
| HTML snippet highlight | Custom HTML builder | Port `snippet_html` from sketch (L126-135) | Sketch logic is PyQt-free; transplants verbatim |

---

## Common Pitfalls

### Pitfall 1: RTL Line-Start Orientation in compose()

**What goes wrong:** The "starts line" checkbox is on the RIGHT in the builder UI (Hebrew line starts on the right). The `compose()` function attaches `|` as a **leading** pipe to the first token of that row (`toks[0] = "|" + toks[0]`). A developer reading "starts line" might assume it means the LEFT token.

**Why it happens:** Sketch iteration E discovered and fixed this (DESKTOP-INTEGRATION-NOTES iteration E). Earlier iterations had the Start/End labels swapped.

**How to avoid:** In `compose()`, `line_start=True` → prepend `|` to `toks[0]` (engine's leading-pipe convention = `line_start` in `_parse_line_break_query`). `line_end=True` → append `|` to `toks[-1]`. Tests must include a Hebrew fixture that verifies a leading-`|` token produces `line_groups[0].line_start == True` from the round-trip parser.

**Warning signs:** Round-trip test passes but `line_start` and `line_end` are swapped in the output `LineGroup`.

### Pitfall 2: Page-Anchor is Independent of Line-START/END (D-08)

**What goes wrong:** Conflating `page_position='start'` (content starts with the match = first page) with `line_start=True` (first word of a line). A first-row with `line_start=True AND page_position='start'` is valid: the match must be at a line-start AND at the start of the document.

**Why it happens:** Both are "start" concepts but operate at different granularities.

**How to avoid:** `page_position` lives on `SideQuery` (applies to the whole query). `line_start`/`line_end` lives on each `BuilderRow` (applies per line). `compose()` emits the `|` syntax from `BuilderRow`; `page_position` is returned as the third element of `compose()` for the adapter to pass as `text_position`.

### Pitfall 3: VS-sourced Candidate Dedup Key

**What goes wrong:** VS candidates have `uid="{sid}|vs"` and `display.img` is set to `1` (placeholder, not a real page). If dedup uses `display.img` directly, all VS candidates from the same sys_id look like page 1 duplicates.

**How to avoid:** `page_of(res)` handles the `_P{N}` uid pattern as fallback. For `uid="{sid}|vs"`, neither fallback finds a page number — `page_of` returns `None`. The dedup key becomes `(sys_id, None)`. Only one VS-only candidate per sys_id survives dedup, which is correct (VS provides one entry per sys_id).

### Pitfall 4: Bracket-Free Query + Bracket-Prefixed Corpus Content (R-02)

**What goes wrong:** A scholar queries for `שהדותא` (a clean word); the corpus page starts with `]שהדותא`. Without bracket stripping, `regex.search(content)` would match, but the line-start position check would see `]` before the word and reject it.

**How to avoid:** The engine handles this automatically via `_strip_brackets(content)` when `_query_has_brackets(query_str)` is False. The shared module's self-match detection and snippet centering should apply the same logic. Use `_strip_brackets` (importable from `genizah_core`) when doing any position-sensitive text operations in the module.

### Pitfall 5: `compose()` Returns `None` for Single-Row / Anchor-Only Queries

**What goes wrong:** If `compose()` returns `(None, None, None)` (all rows empty), the caller passes `None` to `execute_search` and gets an exception.

**How to avoid:** The adapter layer must check for `None` query before calling `execute_search`. Document the contract: `compose()` returns `None` when all rows have empty terms.

### Pitfall 6: VS Coverage is ~50% — Never a Negative Signal

**What goes wrong:** A candidate with no VS result is displayed with "VS: None" or flagged as suspicious.

**Why it happens:** VS only covers 129,456 / 255,723 manuscripts (~50% catalog, ~60% transcribed corpus). No VS row means no precomputed pair, not "visually dissimilar."

**How to avoid:** `Candidate.vs_score: float | None = None` with the convention `None` = "no VS data" (distinct from `0.0` = "dissimilar"). The VS tooltip from the sketch: "Blank means there is NO precomputed pair for these two fragments."

---

## Validation Architecture

Nyquist validation is enabled (`workflow.nyquist_validation: true` in `.planning/config.json`).

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing, all tests in `tests/`) |
| Config file | none (pytest discovers via `tests/` directory) |
| Quick run command | `pytest tests/test_joins_lab.py -x` |
| Full suite command | `pytest tests/ -x` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SC#1 | `compose()` round-trips via `_parse_line_break_query` | unit | `pytest tests/test_joins_lab.py::TestCompose -x` | No — Wave 0 |
| SC#1 | RTL: `line_start=True` → leading `\|` on first token | unit | `pytest tests/test_joins_lab.py::TestCompose::test_line_start_leading_pipe -x` | No — Wave 0 |
| SC#1 | Page anchor: `page_position='start'` returned from `compose()` | unit | `pytest tests/test_joins_lab.py::TestCompose::test_page_position_start -x` | No — Wave 0 |
| SC#1 | `page_position` only valid on first/last row | unit | `pytest tests/test_joins_lab.py::TestCompose::test_page_position_enforced -x` | No — Wave 0 |
| SC#2 | AND: base filtered to `(sid, p±1)` in B-set | unit | `pytest tests/test_joins_lab.py::TestCrossSide::test_and_narrows -x` | No — Wave 0 |
| SC#2 | OR: base + neighbor pages of B-set | unit | `pytest tests/test_joins_lab.py::TestCrossSide::test_or_widens -x` | No — Wave 0 |
| SC#2 | `resolve_other_side_pages`: first→{p+1}, last→{p-1}, mid→{p-1,p+1} | unit | `pytest tests/test_joins_lab.py::TestResolveOtherSide -x` | No — Wave 0 |
| SC#3 | Dedup collapses same `(sys_id, page)` across raw results | unit | `pytest tests/test_joins_lab.py::TestDedup::test_dedup_same_page -x` | No — Wave 0 |
| SC#3 | VS-only candidate uses `(sys_id, None)` key | unit | `pytest tests/test_joins_lab.py::TestDedup::test_vs_uid_key -x` | No — Wave 0 |
| SC#4 | Merge: both-first → text-only → VS-only ordering | unit | `pytest tests/test_joins_lab.py::TestMerge::test_ordering -x` | No — Wave 0 |
| SC#4 | Merge: overlap annotated with `_via_vs=True` + `vs_rank` | unit | `pytest tests/test_joins_lab.py::TestMerge::test_overlap_annotated -x` | No — Wave 0 |
| SC#5 | Self-match: anchor sys_id found in raw results → `True` | unit | `pytest tests/test_joins_lab.py::TestSelfMatch -x` | No — Wave 0 |
| SC#5 | `snippet_html`: centered on first regex match | unit | `pytest tests/test_joins_lab.py::TestSnippet -x` | No — Wave 0 |
| SC#5 | `page_of`: `display.img` path + `_P{N}` uid fallback | unit | `pytest tests/test_joins_lab.py::TestPageOf -x` | No — Wave 0 |
| SC#6 | Static import: no PyQt symbols | unit (AST/import) | `pytest tests/test_joins_lab.py::TestStaticImport -x` | No — Wave 0 |
| SC#6 | Static import: no `fist_data` sqlite3.connect | unit (grep/AST) | `pytest tests/test_joins_lab.py::TestStaticImport::test_no_fist_data_direct -x` | No — Wave 0 |

### FakeSearchExecutor Design

```python
class FakeSearchExecutor:
    """Test double for SearchExecutor Protocol — class-based, matches project conventions."""

    def __init__(self, results=None, browse_pages=None, meta=None, library=None):
        self._results = results or []
        self._browse_pages = browse_pages or {}  # (sys_id, p_num) -> dict
        self._meta = meta or {}                  # sys_id -> (shelfmark, title)
        self._library = library or {}            # sys_id -> library_code
        self.calls = []                          # record for assertions

    def execute_search(self, query_str, mode, gap, **kwargs) -> list[dict]:
        self.calls.append(("execute_search", query_str, kwargs))
        return self._results

    def get_browse_page(self, sys_id, p_num=None, **kwargs) -> dict | None:
        self.calls.append(("get_browse_page", sys_id, p_num))
        return self._browse_pages.get((sys_id, p_num))

    def get_meta_for_id(self, sys_id) -> tuple[str, str]:
        return self._meta.get(sys_id, ("Unknown", ""))

    def get_library_for_id(self, sys_id) -> str:
        return self._library.get(sys_id, "")
```

This exactly matches the `MockMetadataManager` / `MagicMock` pattern used in existing tests (`tests/test_fjms_joins_integration.py`, `tests/test_refinement.py`). [VERIFIED: existing test files]

### Sampling Rate

- **Per task commit:** `pytest tests/test_joins_lab.py -x`
- **Per wave merge:** `pytest tests/ -x`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps

- `tests/test_joins_lab.py` — covers all SC#1..SC#6 (does not exist; create in Wave 0 of the first plan)
- No framework install needed — pytest is already the project standard

---

## Security Domain

This phase builds a pure logic module with no HTTP endpoints, no user authentication, no cryptography, and no new data persistence. The only external data access flows through pre-existing shared services (`visual_similarity_service`, `fjms_service`) which are already in production.

Applicable ASVS categories: V5 (Input Validation) — `compose()` accepts `BuilderRow.term` from a trusted in-process source (the desktop UI or web form). The engine's own input sanitization (`strip_search_diacritics` at `execute_search` entry) handles any malformed query strings. No additional validation required at the shared-module boundary.

---

## Environment Availability

This phase is purely code/config changes building on the existing codebase. No new external dependencies.

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.10+ | `match` / `dataclasses` / `Protocol` | Yes | 3.10+ (project requirement) | — |
| `genizah_core._parse_line_break_query` | SC#1 round-trip tests | Yes | module-level pure fn | — |
| `shared/visual_similarity_service.py` | VS merge (Phase 109, referenced in SC#4 tests) | Yes | existing production | — |
| `shared/fjms_service.py` | Material enrichment (SC#6 test verifies no direct sqlite) | Yes | existing production | — |

---

## State of the Art

| Old Approach (sketch) | Production Approach (Phase 106) | Change |
|----------------------|--------------------------------|--------|
| Direct `sqlite3.connect` to `fist_data/*.db` in sketch helpers (`meas_for`, `vs_score`) | All data through `shared/visual_similarity_service` + `shared/fjms_service` | D-05 |
| PyQt `QThread` wrapping for every async unit | Pure functions; I/O-bound cross-side wrapped in `FakeSearchExecutor` for tests | D-06 |
| Raw `dict` results passed through all layers | `dict → Candidate` normalizer; single source of truth (D-02) | D-02/D-04 |
| `QueryBuilder.compose()` returns `(query_str, responsa_options)` 2-tuple | Returns `(query_str, responsa_options, page_position)` 3-tuple (D-08) | D-08 |
| `uid` as dedup key directly | Canonical `(sys_id, page)` key; `page_of(res)` as the single page extractor | D-02/SC#3 |

---

## Assumptions Log

All claims in this research were verified against live source code or the frozen sketch. No `[ASSUMED]` claims.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| — | — | — | — |

**This table is empty.** All claims were verified via code reads or headless import test.

---

## Open Questions (RESOLVED)

1. **Exact field names for `Candidate` dataclass** — RESOLVED: Plan 01 uses FLAT Candidate fields matching the normalizer's output (`sys_id`, `page`, `uid`, `shelfmark`, `title`, `library_code`, `full_text`, `snippet`, `highlight_pattern`, `score`, `scope`, the `via_*`/`is_anchor_self` provenance flags, `vs_rank`, `vs_score`), not a nested raw `display` dict. Flat is the testable choice and is the single source of truth produced by `normalize_candidate()`. (Claude's Discretion per CONTEXT.md.)
   - Original framing: store raw `display` dict vs normalize flat — flat is testable; nested preserves round-trip to result dict format. A `to_result_dict()` reverse converter can be added later if a round-trip back to result-dict shape is needed.

2. **`page_position` constraint enforcement location** — RESOLVED: Plan 01 splits enforcement across BOTH layers. `SideQuery.__post_init__` validates the VALUE domain (must be `'start'`|`'end'`|`None`, else ValueError — fast fail at model construction). `compose()` then enforces PLACEMENT: it raises ValueError if `page_position == 'start'` but the first row has no non-empty content, or `page_position == 'end'` but the last row has no non-empty content (D-08: page-START anchors the first line, page-END the last line; anchoring an empty/missing line is meaningless). (Claude's Discretion per CONTEXT.md.)
   - Original framing: enforce in `compose()` (raise ValueError on invalid) vs the domain-model constructor — answered as both: value domain in `__post_init__`, placement in `compose()`.

---

## Sources

### Primary (HIGH confidence)
- `genizah_core.py:5811` — `_parse_line_break_query` source (verified importable, headless)
- `genizah_core.py:8001-8219` — `_execute_line_break_search` full implementation (R-01 resolution)
- `genizah_core.py:8298-8365` — `execute_search` + line-break dispatch path (R-01 resolution)
- `genizah_core.py:6342,6352` — `_query_has_brackets`, `_strip_brackets` (R-02 resolution)
- `genizah_core.py:3695,3731` — `get_meta_for_id`, `get_library_for_id` signatures
- `genizah_core.py:9483-9578` — `get_browse_page` signature + return dict shape
- `shared/visual_similarity_service.py:97-128` — `get_suggestions` return shape
- `shared/fjms_service.py:2925-2961` — `get_measurements` return shape
- `.planning/spikes/002-assisted-join-workbench/sketch/join_workbench.py.txt` — executable spec (lines cited by unit)
- `.planning/spikes/002-assisted-join-workbench/DESKTOP-INTEGRATION-NOTES.md` — verified reuse-map
- `.planning/spikes/002-assisted-join-workbench/CODEX-PRODUCTIONIZE-CRITIQUE.md` — architecture decisions
- `.planning/spikes/002-assisted-join-workbench/SPIKE-FINDINGS.md` — VS coverage quantification

### Secondary (MEDIUM confidence)
- `.planning/phases/106-.../106-CONTEXT.md` — locked decisions D-01 through D-13
- `.planning/REQUIREMENTS.md` § Design-Critique Conclusions — JWB-10/11/12 definitions
- `.planning/ROADMAP.md` § Phase 106 — 6 success criteria

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — no new deps; uses existing shared services verified in production
- R-01 (page-anchor execution path): HIGH — traced through `execute_search` → `_execute_line_break_search` source code
- R-02 (bracket handling): HIGH — `_query_has_brackets` + `_strip_brackets` implementation read directly
- Architecture patterns: HIGH — cross-referenced with sketch + CODEX-PRODUCTIONIZE-CRITIQUE
- Test strategy: HIGH — follows existing project test conventions verified in `tests/`

**Research date:** 2026-06-03
**Valid until:** 2026-08-03 (engine paths are stable; VS/FJMS service signatures stable)

---

## RESEARCH COMPLETE

**Phase:** 106 — Joins Lab Shared Core
**Confidence:** HIGH

### Key Findings

1. **R-01 RESOLVED: ONE engine call.** `execute_search(query_str_with_pipe_syntax, ..., text_position='start'|'end'|None)` covers both the line-break and the page-anchor cases. The `text_position` parameter is forwarded through `execute_search` into `_execute_line_break_search` where it is applied as a post-regex position filter (lines 8142-8153). No intersection step needed. `compose()` returns a 3-tuple `(query_str, responsa_options, page_position)`.

2. **R-02 RESOLVED: Engine auto-strips brackets.** When the query is bracket-free (typical scholar query), `_execute_line_break_search` applies `_strip_brackets(content)` before the regex match at line 8125. Leading tear-bracket tokens (e.g., `]שהדותא`) are stripped before line-start position checking. Self-match detection and snippet centering in the shared module should use the same `_strip_brackets` / `_query_has_brackets` functions (importable from `genizah_core`).

3. **`_parse_line_break_query` is safe to import without engine init.** Confirmed via headless Python import test. The round-trip test (SC#1) can import it directly.

4. **Result dict shape fully documented.** The `dict → Candidate` normalizer can be written from the verified keys. `page_of(res)` handles both the `display.img` path and the `_P{N}` uid fallback; VS-sourced results with `uid="{sid}|vs"` get `page=None` which is the correct dedup key.

5. **All six logic units transplant as pure functions.** The sketch's `QueryBuilder.compose`, `_CrossSideWorker.run` (AND/OR logic), `_on_results` dedup, `_maybe_assemble` merge, `_anchor_matched`, `snippet_html`, `snippet_plain`, `page_of` — all are PyQt-free inner logic. The QThread wrappers strip away; the pure function signatures are ready to codify.

6. **No new dependencies.** `shared/joins_lab.py` uses only stdlib + existing shared services. VS coverage is ~50% (129,456 / 255,723 catalog); plan documents this as a "supplementary signal only" constraint in `Candidate.vs_score` semantics.

### File Created

`.planning/phases/106-joins-lab-shared-core-pure-logic-service-adapters-unit-tests/106-RESEARCH.md`

### Confidence Assessment

| Area | Level | Reason |
|------|-------|--------|
| Standard stack | HIGH | No new deps; all existing shared services verified |
| R-01 (page-anchor + line-break path) | HIGH | Traced through execute_search → _execute_line_break_search source code with line citations |
| R-02 (bracket handling) | HIGH | _query_has_brackets + _strip_brackets implementation read and understood |
| SearchExecutor Protocol | HIGH | All 4 methods verified in genizah_core.py with line numbers |
| Result dict shape | HIGH | Verified from _execute_line_break_search output construction + VS worker output |
| Test strategy | HIGH | Matches existing class-based pytest conventions in tests/ |

### Open Questions

- Exact `Candidate` dataclass field names (flat vs nested `display` dict) — Claude's discretion per CONTEXT.md
- `page_position` constraint enforcement location (`compose()` raise vs `SideQuery.__post_init__`) — Claude's discretion

### Ready for Planning

Research complete. Planner can now create PLAN.md files for Phase 106.
