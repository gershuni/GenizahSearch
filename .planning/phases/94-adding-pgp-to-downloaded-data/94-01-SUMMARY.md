---
phase: 94-adding-pgp-to-downloaded-data
plan: 01
subsystem: export
tags: [xlsx, pgp, nli, fjms, dossier, openpyxl, shared-module, tdd]

# Dependency graph
requires:
  - phase: 87-foundations
    provides: web/safe_storage chokepoint (Phase 87 invariant respected — Wave 1 does not touch web/)
  - phase: v6.0.0-local-data
    provides: shared/document_service.get_document_for_fragment + shared/fjms_service + shared/nli_crossref_service sidecar readers
provides:
  - shared/export_dossier.py module — 4 lookup helpers + 2 row emitters + 2 header constants
  - shared_export_utils.build_rich_snippet_cell helper — extracted from desktop write_rich_cell inner closure (D-14)
  - _split_pgp_languages internal fixing the SUPERSEDED-v2 comma-string-becomes-char-iteration bug (T-94-04)
  - meta_resolver callable contract — primitive 4-key dict; replaces opaque meta_mgr to prevent web/desktop drift (Codex SHOULD-FIX 8)
  - Stable public API surface for Waves 2/3/4 to consume without re-exploration
affects: [Wave 2 web state plumbing, Wave 3 web xlsx restructure, Wave 4 desktop xlsx parity]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Narrow whitelist projection pattern: each helper projects only the dossier-relevant keys, never re-exposing upstream service dict fields (D-02 boundary)"
    - "Per-helper inline exception resilience (try/except → warning log → None/[]) — replaces shared/browse_service's outer _wrap_with_timeout because dossier has no timeout layer"
    - "Module-scope factory imports + monkeypatch targets at 'shared.export_dossier.<name>' (MUST-FIX 94-01-A) — lazy in-function imports would defeat tests"
    - "First-non-empty per field aggregation across multi-team catalog records (catalog_summary_for_sys_id)"
    - "MetaResolver callable contract (Codex SHOULD-FIX 8) — opaque meta_mgr replaced with sys_id -> Optional[{shelfmark, title, library_code, library_name}]"
    - "Module-level header constants paired with row emitters; tests assert len(row) == len(headers) at construction time"
    - "Late openpyxl import inside build_rich_snippet_cell — pay rich-text cost only when actually rendering markers"

key-files:
  created:
    - shared/export_dossier.py
    - tests/test_export_dossier.py
    - tests/test_shared_rich_snippet.py
  modified:
    - shared_export_utils.py

key-decisions:
  - "MUST-FIX 94-01-A applied: 3 service factory functions imported at module scope (not lazy in-function) so test monkeypatches at 'shared.export_dossier.<name>' intercept the lookup. Per-test isolation via monkeypatch + SimpleNamespace-equivalent fake services."
  - "MUST-FIX 94-01-B applied: unknown library codes pass through genizah_core.get_library_display unchanged (LIBRARY_CODES.get(code, code) graceful fallback) — pinned by test_unknown_library_code_graceful_fallback."
  - "MUST-FIX 94-01-C applied: D-02 prohibition enforced BEHAVIORALLY (test injects page_section_text/transcription/full_text into upstream dict, asserts helper drops them) instead of by literal grep against the module — lets docstrings name prohibited fields without violating the rule."
  - "Codex MUST-FIX 1 applied: bibliography_for_sys_id projects REAL FJMS field names {running_title, title_year, mention_page, article_name, article_author_eng, catalog_acronym}; the SUPERSEDED-v2-invented {Author, Publisher, Source Name} are gone."
  - "Codex MUST-FIX 2 applied: column header is 'NLI Catalog Entry' (NOT 'NLI Description'); get_catalog_entry returns Neubauer-Cowley reference strings."
  - "Codex MUST-FIX 3 applied: catalog_summary_for_sys_id uses FjmsService.get_catalog_records exclusively; the detail variant (which loads full_texts) is never invoked — confirmed by _FakeFjms.get_catalog_detail raising AssertionError if called."
  - "Codex MUST-FIX 4 applied: build_manuscript_row calls 3 helpers (PGP + NLI + Catalog); bibliography rows live on the separate Bibliography sub-sheet built by build_bibliography_rows."
  - "Codex SHOULD-FIX 7 applied: MANUSCRIPT_HEADERS (14) + BIBLIOGRAPHY_HEADERS (8) are module-level List[str] constants; tests pin len(row) == len(headers)."
  - "Codex SHOULD-FIX 8 applied: meta_resolver callable replaces opaque meta_mgr to prevent silent web/desktop drift if either app evolves its meta interface."
  - "Codex SHOULD-FIX 9 applied: library name resolved via genizah_core.get_library_display(lang='en') hard-pin — NEVER routed through web.translations.get_language()."
  - "Codex SHOULD-FIX 10 applied: public-API helpers have no underscore prefix (pgp_subset_for_sys_id, nli_subset_for_sys_id, etc.)."
  - "Codex SHOULD-FIX 12 applied: 18+ tests cover missing-sidecar / empty-input / service-exception / no-transcription-leak / comma-split-languages per helper."
  - "Aggregation strategy for catalog_summary documented in helper docstring: first non-empty per field across all FJMS catalog records. Rationale: multi-team cataloging often partial — pick first scholar-provided value per field independently rather than picking one record verbatim."
  - "Helper output dicts are Python primitives ONLY — no openpyxl objects (D-14 cleavage). The build_rich_snippet_cell helper lives in shared_export_utils.py, not shared/export_dossier.py."
  - "build_rich_snippet_cell sanitize-first ordering: sanitize_fn runs BEFORE the '*'-split so formula-injection prefix is preserved into the first split part (T-94-01 mitigation)."
  - "Late openpyxl import inside build_rich_snippet_cell function body — avoids paying the rich-text dependency cost for callers that never render snippets."

patterns-established:
  - "Pattern A: narrow whitelist projection per helper — each helper returns only the dossier-relevant keys; upstream service dicts may contain extra content but the helper drops them (D-02 boundary)."
  - "Pattern B: inline try/except + warning log + None/[] return — replaces the analog's outer timeout wrapper because dossier has no outer wrapper."
  - "Pattern C: module-scope factory imports as the canonical monkeypatch surface for tests (MUST-FIX 94-01-A documented in module docstring)."
  - "Pattern D: MetaResolver callable contract for cross-app meta plumbing — primitive 4-key dict, NOT an object reference."
  - "Pattern E: paired module-level headers + row builders with length assertions in tests — prevents header/row drift across Waves 3 + 4."

requirements-completed: [EXPORT-META-02, EXPORT-META-03, EXPORT-META-05, EXPORT-META-08]

# Metrics
duration: ~25min
completed: 2026-05-20
---

# Phase 94 Plan 01: Wave 1 Shared Dossier Primitives Summary

**Built the load-bearing shared/export_dossier.py module (4 helpers + 2 row emitters + 2 header constants) plus the build_rich_snippet_cell helper, giving Waves 2/3/4 a stable, tested API for the Manuscripts + Bibliography sub-sheets and main-sheet rich-text rendering.**

## Performance

- **Duration:** ~25 min
- **Tasks:** 3 (Tasks 1 + 2 combined into one commit; Task 3 separate)
- **Files created:** 3 (`shared/export_dossier.py`, `tests/test_export_dossier.py`, `tests/test_shared_rich_snippet.py`)
- **Files modified:** 1 (`shared_export_utils.py` — appended `build_rich_snippet_cell`)
- **Net lines added:** ~1,630 (528 production + 1,102 test).
- **Net tests added:** 69 (61 dossier + 8 rich-snippet).

## Accomplishments

- Shipped `shared/export_dossier.py` with the public API both web (Wave 3) and desktop (Wave 4) will import: 4 lookup helpers, 2 row emitters, 2 header constants, plus `_split_pgp_languages` internal fixing the comma-string bug from SUPERSEDED-v2.
- Shipped `build_rich_snippet_cell(text, sanitize_fn)` in `shared_export_utils.py` — extracted from desktop's `write_rich_cell` inner closure at `genizah_app.py:17988-18021` per D-14 so web's main sheet (Wave 3) can adopt the same red+bold highlight rendering and desktop (Wave 4) can switch its inner closure to the shared helper.
- Every Codex MUST-FIX folded in: real FJMS bib field names (MUST-FIX 1); NLI Catalog Entry header (MUST-FIX 2); narrow `get_catalog_records` query only — `get_catalog_detail` never invoked (MUST-FIX 3); `build_manuscript_row` calls only PGP+NLI+Catalog, never bibliography (MUST-FIX 4).
- Every Codex pre-flight critique applied: module-scope factory imports (MUST-FIX 94-01-A); unknown-library-code graceful fallback (MUST-FIX 94-01-B); behavioral D-02 assertion replacing the self-contradictory literal grep (MUST-FIX 94-01-C).
- Wave 1 stays strictly module-creation — zero call-site wiring; Waves 2/3/4 each get a stable API to import without exploration.

## Task Commits

1. **Tasks 1 + 2 (combined): create shared/export_dossier.py with 4 lookup helpers, 2 row emitters, 2 header constants, _split_pgp_languages internal** — `46340041` (feat)
   - Tasks 1 and 2 share the same artifact (`shared/export_dossier.py` + `tests/test_export_dossier.py`); combining them avoided an intermediate state where the row emitters' tests would reference undefined headers. Per the GSD atomicity rule, the commit message documents both tasks and lists every helper + emitter + constant individually.
2. **Task 3: extract build_rich_snippet_cell into shared_export_utils** — `27fa4e04` (feat)

## Files Created/Modified

- `shared/export_dossier.py` (NEW, 528 lines) — module docstring (D-02 boundary, D-04 English-only contract, MUST-FIX 94-01-A explanation, Codex MUST-FIX disposition) + 3 module-scope factory imports + module logger + MANUSCRIPT_HEADERS (14) + BIBLIOGRAPHY_HEADERS (8) + MetaResolver type alias + `_split_pgp_languages` internal + 4 lookup helpers (`pgp_subset_for_sys_id`, `nli_subset_for_sys_id`, `catalog_summary_for_sys_id`, `bibliography_for_sys_id`) + 2 row emitters (`build_manuscript_row`, `build_bibliography_rows`).
- `tests/test_export_dossier.py` (NEW, 980 lines) — 8 test classes covering all 4 helpers, both row emitters, the internal `_split_pgp_languages`, the module header constants, and the unknown-library-code graceful fallback (61 tests total).
- `tests/test_shared_rich_snippet.py` (NEW, 122 lines) — 8 tests covering plain text / single highlight / multi-highlight / sanitize-first-ordering / empty input / None input / no-sanitize-fn fallback.
- `shared_export_utils.py` (MODIFIED, +62 lines) — appended `build_rich_snippet_cell(text, sanitize_fn=None)` function in a new section between `remove_highlight_markers` and `make_safe_filename` (after the existing text-utility functions, before the filename-sanitization section). Late openpyxl import inside the function body.

## Public API Surface (Ready for Wave 2/3/4 Consumption)

```python
# From shared.export_dossier:
MANUSCRIPT_HEADERS: List[str]                                   # 14 columns
BIBLIOGRAPHY_HEADERS: List[str]                                  # 8 columns
MetaResolver = Callable[[str], Optional[Dict[str, Any]]]         # type alias

pgp_subset_for_sys_id(sys_id: str) -> Optional[dict]             # 6 keys or None
nli_subset_for_sys_id(sys_id: str) -> Optional[dict]             # 2 keys or None
catalog_summary_for_sys_id(sys_id: str) -> Optional[dict]        # 4 keys or None
bibliography_for_sys_id(sys_id: str) -> List[dict]               # N x 6 keys

build_manuscript_row(sys_id, meta_resolver, lang='en') -> List[Any]    # 14 cells
build_bibliography_rows(sys_id, meta_resolver) -> List[List[Any]]      # 0..N x 8 cells

# From shared_export_utils:
build_rich_snippet_cell(text, sanitize_fn=None) -> Union[str, CellRichText]
```

## Codex MUST-FIX Folded In (Confirmation)

| ID | Item | Pin |
|---|---|---|
| MUST-FIX 1 | Real FJMS bib field names | `bibliography_for_sys_id` projects `{running_title, title_year, mention_page, article_name, article_author_eng, catalog_acronym}` — tested by `TestBibliography::test_real_fjms_field_names_only` |
| MUST-FIX 2 | NLI Catalog Entry naming | `MANUSCRIPT_HEADERS[10] == 'NLI Catalog Entry'` — tested by `TestModuleHeaders::test_manuscript_headers_includes_nli_catalog_entry` |
| MUST-FIX 3 | get_catalog_records NOT get_catalog_detail | `_FakeFjms.get_catalog_detail` raises `AssertionError` if invoked — verified zero callers in `catalog_summary_for_sys_id`; `grep get_catalog_detail shared/export_dossier.py` returns nothing |
| MUST-FIX 4 | build_manuscript_row does NOT call bibliography | `TestBuildManuscriptRow::test_does_not_call_bibliography_helper` traps `bibliography_for_sys_id` to raise `AssertionError` and confirms it's never invoked |
| 94-01-A | Module-scope factory imports | `grep -E "^from shared\\.(document_service|nli_crossref_service|fjms_service) import"` returns all 3 — verified |
| 94-01-B | Unknown library code graceful fallback | `TestBuildManuscriptRow::test_unknown_library_code_graceful_fallback` passes `'UNKNOWN_XYZ'` and asserts row[2] == 'UNKNOWN_XYZ' |
| 94-01-C | Behavioral D-02 assertion | `TestPgpSubset::test_no_transcription_text_leak` injects `page_section_text`/`transcription`/`full_text` into upstream dict and asserts they're absent from helper output |

## D-02 Boundary Regression Confirmation

The strict prohibition on transcription text in NEW dossier surfaces (Manuscripts sub-sheet, Bibliography sub-sheet, JSON envelope additions) is enforced behaviorally:

- `TestPgpSubset::test_no_transcription_text_leak` — pgp upstream dict containing `page_section_text` / `transcription` / `full_text` confirms helper output dict drops them (whitelist projection).
- `TestBibliography::test_no_transcription_leak` — bibliography upstream dict containing `comment` / `note_for_display` / `catalog_entry` confirms helper output dicts drop them (6-key whitelist).
- `TestCatalogSummary::test_uses_get_catalog_records_not_get_catalog_detail` — `_FakeFjms.get_catalog_detail` raises `AssertionError` if invoked, proving the dossier path never reaches into the detail query that loads `full_texts`.

## Decisions Made

- Tasks 1 + 2 combined into a single commit because both deliverables live in the same `shared/export_dossier.py` + `tests/test_export_dossier.py` files. Splitting them into two commits would have required an intermediate state where the row-builder tests reference yet-undefined module-level header constants. The combined commit message lists every helper + emitter + constant individually for traceability.
- For Task 3, the rich-text helper lives in `shared_export_utils.py` (Pattern 2.4 recommendation b in 94-PATTERNS.md) NOT in `shared/export_dossier.py` (rejected option c). Rationale: dossier sub-sheets are plain text per D-14; the rich-text helper has no business living next to dossier projections.
- The aggregation strategy in `catalog_summary_for_sys_id` is *first non-empty per field across records*, NOT *first record's verbatim shape*. Multi-team catalog records are often partial per-field; picking field-by-field gives a more complete summary. Documented in helper docstring.
- The `lang` parameter on `build_manuscript_row` is named in the signature for forward-compatibility (caller's downstream sheet-view direction decision in Wave 3/4) but is intentionally unused in the row builder itself — row content is always English per D-04. Documented in helper docstring.

## Deviations from Plan

**None** — plan executed exactly as written. All MUST-FIX items (94-01-A, 94-01-B, 94-01-C) from the Codex pre-flight critique were folded into the plan before execution and applied directly in implementation.

Minor stylistic refinement: the Task 1 acceptance criteria included `grep "get_catalog_detail" shared/export_dossier.py` returns nothing. This conflicted with the natural urge to document the prohibition by name in the module docstring. The docstrings were rephrased to refer to "the detail variant" / "the narrow query" without naming the prohibited method, preserving the literal grep contract.

The build error reported by ruff on first run (`F401` for unused `SimpleNamespace` + `pytest` imports in the test file) was auto-fixed (Rule 1 — bug introduced by my own initial write) by removing those imports; the file uses `monkeypatch` (pytest fixture, no import needed) and never directly references `SimpleNamespace`.

## Wave 1 → Wave 2 Hand-off

Wave 2's scope (per `94-CONTEXT.md` D-15):

- `shared/search_serializer.py:_serialize_item` adds `is_printed` and `has_pgp` keys (additive; envelope schema_version stays 1 per D-11).
- `web/export_state.py:set_search_export` accepts new kwargs `transcription_sys_ids`, `printed_ids`, `result_domains`.
- New `update_search_export_enrichment(...)` helper for post-enrichment patching.
- `web/pages/search.py` — 3 `set_search_export` call sites + 1 new `update_search_export_enrichment` call site.
- Parallels D-10 regression test (`tests/test_parallels_envelope_no_pgp_keys.py`) asserting `_to_parallels_envelope_item` does NOT inherit the new keys.

Wave 2 does NOT need to import `shared.export_dossier` — it only plumbs the per-search signals through state. Wave 3 (web xlsx) and Wave 4 (desktop xlsx) are the consumers.

## Self-Check: PASSED

**Created files verified to exist:**

- `shared/export_dossier.py` — FOUND (528 lines)
- `tests/test_export_dossier.py` — FOUND (980 lines)
- `tests/test_shared_rich_snippet.py` — FOUND (122 lines)

**Modified files verified to contain new symbol:**

- `shared_export_utils.py` — contains `def build_rich_snippet_cell` (FOUND)

**Commits verified to exist in `git log --oneline --all`:**

- `46340041` (feat(94-01): add shared/export_dossier.py — 4 lookup helpers + 2 row emitters + 2 header constants) — FOUND
- `27fa4e04` (feat(94-01): extract build_rich_snippet_cell into shared_export_utils) — FOUND

**Verification commands run green:**

- `python -m pytest tests/test_export_dossier.py tests/test_shared_rich_snippet.py -q` → 69 passed
- `python -m pytest tests/test_export_dossier.py tests/test_shared_rich_snippet.py tests/test_search_serializer.py tests/test_export_state_selection.py -q` → 121 passed
- `python -m pytest tests/test_no_raw_storage_access.py -q` → 6 passed (Phase 87 multitenant invariant unaffected)
- `python -m ruff check shared/export_dossier.py shared_export_utils.py tests/test_export_dossier.py tests/test_shared_rich_snippet.py` → All checks passed
- `python -c "from shared.export_dossier import pgp_subset_for_sys_id, nli_subset_for_sys_id, catalog_summary_for_sys_id, bibliography_for_sys_id, build_manuscript_row, build_bibliography_rows, MANUSCRIPT_HEADERS, BIBLIOGRAPHY_HEADERS; from shared_export_utils import build_rich_snippet_cell; print('OK')"` → OK
