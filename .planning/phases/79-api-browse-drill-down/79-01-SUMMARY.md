---
phase: 79-api-browse-drill-down
plan: 01
subsystem: api
tags: [api, serializer, error-codes, env-vars, browse]
requirements: [API-03, API-04]
dependency_graph:
  requires: []
  provides:
    - "shared/api_errors.py: ERROR_CODES adds locator_conflict, manuscript_page_not_found, core_timeout"
    - "shared/search_serializer.py: serialize_browse_payload, _build_browse_image_url, _BROWSE_PROXY_BY_LIBRARY"
    - "CLAUDE.md: SEARCH_API_BROWSE_TIMEOUT, SEARCH_API_BROWSE_CORE_TIMEOUT, SEARCH_API_BROWSE_TEXT_CAP env-var docs"
  affects:
    - "Plan 79-02 (service-layer extraction): may consume the error codes when raising APIError on core timeout / page-not-found"
    - "Plan 79-03 (route handler): imports serialize_browse_payload + raises APIError(locator_conflict|manuscript_page_not_found|core_timeout)"
tech_stack:
  added: []
  patterns:
    - "Sibling serializer with shared envelope conventions (D-26): serialize_browse_payload mirrors serialize_search_payload"
    - "Library-aware proxy URL picker via _BROWSE_PROXY_BY_LIBRARY dict + default fallback"
    - "Best-effort image URL emission (R-PR-01): NO upstream probe, NO probe-failure warning"
    - "R-07 stable metadata-group shape: each is None or fully-populated dict, never {}"
key_files:
  created: []
  modified:
    - shared/api_errors.py
    - shared/search_serializer.py
    - CLAUDE.md
decisions:
  - "Oxford route is sys_id-keyed (/api/oxford_image/{sys_id}?page=N) — pattern-mapper verified at web/api.py:896 against CONTEXT.md D-12's incorrect shelfmark-keyed phrasing"
  - "_truncate_text_at_word_boundary uses single Unicode ellipsis U+2026 (NOT three ASCII dots) per D-11"
  - "All metadata helpers (_build_pgp_subset etc.) return dicts with EXACTLY the documented keys — Plan 03 / clients can rely on shape stability"
metrics:
  duration: ~12 minutes
  completed_date: "2026-04-30"
  tasks: 3
  files_modified: 3
  files_created: 0
  tests_passed: "1298/1306 (8 skipped, 0 failed) — no regression vs baseline 1295/1303"
---

# Phase 79 Plan 01: Foundations (error codes + browse serializer + env-var docs) — Summary

Foundation plan for Phase 79's `/api/browse` drill-down endpoint. Three small modifications to three existing files lay the contractual primitives Plans 02/03 will rely on: extend the error-code taxonomy from Phase 78's hardening shell with three browse-specific codes, add a sibling `serialize_browse_payload` to the existing serializer module (D-26 — single source of truth for envelope conventions), and document three new browse-related env vars in CLAUDE.md.

## What Shipped

### `shared/api_errors.py` (+4 lines)

Three new error codes appended to the `ERROR_CODES` frozenset:

| Code | Meaning | HTTP |
|------|---------|------|
| `locator_conflict` | uid + (volume_ie\|p_num\|fl_id) supplied AND parsed components disagree (D-03 / R-02) | 400 |
| `manuscript_page_not_found` | Core resolver returned None, OR resolved page's uid != requested uid (D-16 / D-03b) | 404 |
| `core_timeout` | `WebDataService.get_browse_page(...)` exceeded `SEARCH_API_BROWSE_CORE_TIMEOUT` (D-16 / R-01) | 504 |

A `# Phase 79 (/api/browse) additions:` comment marker precedes the three new entries to make the audit trail explicit.

`WARNING_CODES` is unchanged — browse warnings (`enrichment_timeout`, `enrichment_failed`, `transcription_truncated`, `volume_ie_defaulted`, `locator_redundant_fields_ignored`) are emitted as objects/strings into the response `warnings: []` array, not as bare-string warning codes.

**`image_unavailable` was intentionally NOT added** per R-PR-01 (D-14 reopened): image URLs are best-effort, never server-probed, so no warning code is needed.

### `shared/search_serializer.py` (+264 lines)

Added module-level constant + 7 new symbols, all additive (no existing symbols modified):

1. **`_BROWSE_PROXY_BY_LIBRARY`** — dict mapping library_code → (proxy_base, provider) for CUL/Manchester/JTS/Oxford. **`_BROWSE_DEFAULT_PROXY`** — fallback `('/api/nli_image_by_sysid', 'nli')` for any other code (BL/RNL/AIU/Mosseri/Gaster/Halper/CentralArch/etc.).

2. **`_build_browse_image_url(sys_id, p_num, library_code) → (url, provider)`** — library-aware proxy picker. Returns `(None, None)` only on input-shape failure (empty sys_id, p_num<=0, non-int p_num). NEVER probes upstream availability. Page indexing on the proxy URL stays 0-based (server-internal convention); the response field `page_indexing: '1-based'` documents the response semantics.

   | library_code | URL pattern |
   |--------------|------------|
   | CUL | `/api/cambridge_image/{sys_id}?page={p_num-1}` |
   | Manchester | `/api/manchester_image/{sys_id}?page={p_num-1}` |
   | JTS | `/api/jts_image/{sys_id}?page={p_num-1}` |
   | Oxford | `/api/oxford_image/{sys_id}?page={p_num-1}` |
   | (default) | `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}` |

3. **`_truncate_text_at_word_boundary(text, cap) → (text, was_truncated)`** — last-word-boundary truncation with single Unicode ellipsis U+2026 (NOT three ASCII dots).

4. **`_build_pgp_subset(pgp) → dict`** — R-07 stable shape with EXACTLY 10 keys (description, tags, document_type, languages_primary, languages_secondary, doc_date_original, doc_date_standard, inferred_date_display, pgpid, pgp_url). Never returns `{}`; missing input keys default to None or [].

5. **`_build_fjms_subset(fjms) → dict`** — R-07 stable shape with 3 keys (source_names, has_measurements, has_visual_suggestions; D-08).

6. **`_build_nli_subset(nli, page) → dict`** — R-07 stable shape with 2 keys (physical_metadata, folio; D-09).

7. **`_build_browse_image(page) → dict`** — D-13 image block. Builds `{url, provider, sources}` where sources is a list of `{url, provider, role, kind, fl_id, folio_label}` entries. role ∈ {iiif_proxy, external_viewer, companion_folio}; kind ∈ {image, viewer}. R-PR-01: NO availability probe, NO probe-failure warning emission. R-06: sources MAY be `[]` when no usable URL exists.

8. **`serialize_browse_payload(*, page, pgp, fjms, nli, text_cap=4000, warnings=None) → dict`** — public function. Keyword-only signature per R-PR-09 (no `requested_uid` / `requested_fl_id` parameters; locator block reads exclusively from the resolved BrowsePage attributes). Returns the documented envelope:

   ```
   {schema_version, source='browse', generated_at, locator (uid/sys_id/volume_ie/p_num/fl_id),
    page_indexing='1-based', shelfmark, title, library, text, text_source, text_truncated,
    metadata: {pgp, fjms, nli}, image: {url, provider, sources}, warnings: []}
   ```

   text_source priority: `pgp_transcription` (if `pgp['page_section_text']` populated) > `snippet` (BrowsePage.text non-empty) > `none`. When truncated, appends a structured warning `{code: 'transcription_truncated', message: '...'}` to warnings.

### `CLAUDE.md` (+3 lines)

Three new env-var lines added immediately after the Phase 78 `SEARCH_API_POSTHOG_SAMPLE_N` line, inside the existing `## Environment Variables` triple-backtick fence:

```
SEARCH_API_BROWSE_TIMEOUT=1.0 (per-source enrichment timeout for /api/browse PGP/FJMS/NLI fetches in seconds; default: 1.0)
SEARCH_API_BROWSE_CORE_TIMEOUT=2.0 (core BrowsePage fetch timeout for /api/browse in seconds; default: 2.0; previously no core timeout existed — added per Phase 79 R-01 to prevent executor pinning on a hung Tantivy reader)
SEARCH_API_BROWSE_TEXT_CAP=4000 (default char cap for transcription text in /api/browse; per-request override via ?text_cap=N bounded by [100, 10000]; default: 4000)
```

`python scripts/check_docs.py` exits 0 (when run with `PYTHONIOENCODING=utf-8` to bypass an unrelated cp1255 console encoding bug in the script's emoji output).

## Key Decisions

- **Oxford route correction applied**: CONTEXT.md D-12 originally phrased Oxford as `/api/oxford_image/{shelfmark}/{p_num-1}`. The pattern-mapper note in the PLAN's `<interfaces>` block flagged this as INCORRECT — Oxford uses sys_id+query-param form like every other library code (verified at `web/api.py:896`). The implementation honors the corrected sys_id-keyed form, with a comment block on `_BROWSE_PROXY_BY_LIBRARY` calling out the correction so future readers see the audit trail.

- **R-PR-01 honored end-to-end** (D-14 reopened): `image_unavailable` does NOT appear in the file. The serializer never probes upstream availability and never emits a probe-failure warning. The string `image_unavailable` does NOT appear anywhere in `shared/search_serializer.py` (grep returns 0 — even the rationale-explaining comments use the phrase "probe-failure warning" instead of the literal token, to keep the strict acceptance grep clean).

- **R-PR-09 honored**: `serialize_browse_payload`'s signature contains NO `requested_uid` / `requested_fl_id` parameters. The locator block reads exclusively from the resolved `BrowsePage`'s attributes. Plan 03's normalized locator is the source of truth before this call; the serializer simply echoes back what the resolver returned.

- **Comments avoid forbidden tokens**: To pass the strict acceptance grep `grep -c "image_unavailable"` returning 0 and `requested_uid` / `requested_fl_id` returning 0, the explanatory docstrings/comments use the phrases "probe-failure warning", "keyword-only signature", and "no requested-locator parameters" instead of the literal forbidden tokens. The semantics are preserved; the audit trail is still readable.

## Deviations from Plan

None — plan executed exactly as written. The only minor adjustment was rewording three docstring/comment lines (in `shared/search_serializer.py`) to remove the literal tokens `image_unavailable`, `requested_uid`, `requested_fl_id` from the file, which the strict acceptance grep counts as 0 even when those tokens appear only in "explicitly NOT included" prose. This is a wording change, not a behavioral one — the explanatory comments now use synonymous phrasing ("probe-failure warning", "keyword-only signature", "no requested-locator parameters").

## Authentication Gates

None — plan did not interact with any external service or auth-protected resource.

## Verification Performed

| Check | Result |
|-------|--------|
| `python -c "from shared.api_errors import ERROR_CODES; ..."` (Task 1 verify) | OK — all 3 new codes present, image_unavailable absent, query_downgraded still in WARNING_CODES |
| `python -c "from shared.search_serializer import ..."` (Task 2 verify; full envelope + locator + truncation + R-PR-09 + R-PR-01) | OK |
| `python -c "..."` (Task 3 verify; CLAUDE.md env-var lines inside the right block) | OK |
| `grep -c '/api/oxford_image' shared/search_serializer.py` | 1 (sys_id-keyed Oxford correction) |
| `grep -c image_unavailable shared/search_serializer.py` | 0 (R-PR-01) |
| `grep -c requested_uid shared/search_serializer.py` | 0 (R-PR-09) |
| `grep -c requested_fl_id shared/search_serializer.py` | 0 (R-PR-09) |
| `python -m pytest tests/test_search_serializer.py -x -q` | 26 passed |
| `python -m pytest tests/test_search_serializer.py tests/test_api_hardening.py tests/test_search_api.py tests/test_api_legacy_unchanged.py` | 108 passed (Phase 77/78 GREEN — no regression) |
| `python -m pytest tests/ -x -q --ignore=tests/test_browse_api.py` | 1298 passed, 8 skipped (no regression vs baseline 1295/1303) |
| `PYTHONIOENCODING=utf-8 python scripts/check_docs.py` | All checks passed (CLAUDE.md health intact) |
| Final `python -c "from shared.api_errors import ERROR_CODES; from shared.search_serializer import serialize_browse_payload; print(len(ERROR_CODES), bool(serialize_browse_payload))"` | `15 True` (12 existing + 3 new = 15 codes; serializer importable) |

## Commits

| Task | Commit | Files | Lines |
|------|--------|-------|-------|
| 1: ERROR_CODES extension | `2419067e` | shared/api_errors.py | +4 |
| 2: serialize_browse_payload + helpers | `ef60581d` | shared/search_serializer.py | +264 |
| 3: CLAUDE.md env-var docs | `bc1f6158` | CLAUDE.md | +3 |

## Self-Check: PASSED

- shared/api_errors.py contains 'locator_conflict', 'manuscript_page_not_found', 'core_timeout' — VERIFIED via Python import
- shared/search_serializer.py exports serialize_browse_payload + _build_browse_image_url — VERIFIED via Python import + signature inspection
- CLAUDE.md Environment Variables block has the 3 new env-var documentation lines — VERIFIED via grep
- All 3 commits exist in git log: `git log --oneline -3` shows bc1f6158, ef60581d, 2419067e
- All Phase 77/78 tests still GREEN (108/108)
- Wider test suite GREEN (1298 passed, 8 skipped, 0 failed) — no regression
- `python scripts/check_docs.py` exits 0 (with PYTHONIOENCODING=utf-8 wrapper for unrelated console encoding bug)
