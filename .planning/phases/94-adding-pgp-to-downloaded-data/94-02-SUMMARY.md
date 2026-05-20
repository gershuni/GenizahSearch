---
phase: 94-adding-pgp-to-downloaded-data
plan: 02
subsystem: export
tags: [state-plumbing, json-envelope, opt-in-keys, multitenant-safe, tdd, wave-2]

# Dependency graph
requires:
  - phase: 87-foundations
    provides: web/safe_storage chokepoint (Phase 87 invariant respected — allowlist still [])
  - phase: 88-state-separation-by-deletion
    provides: D-11 isinstance guard + D-12 copy-on-update conventions (extended to update_search_export_enrichment)
  - plan: 94-01
    provides: shared/export_dossier.py module — Wave 2 does NOT import it (consumed in Waves 3/4)
provides:
  - set_search_export 3 new kwargs (transcription_sys_ids, printed_ids, result_domains) — payload-write surface for the xlsx + JSON export pipeline
  - update_search_export_enrichment(...) sibling helper — async post-enrichment patch path with independent-field semantics
  - _serialize_item opt-in has_pgp/is_printed per-item flags (MUST-FIX 94-02-B: D-11 public-shape stability for /api/search preserved)
  - _extract_sys_id_for_batch shared resolver (MUST-FIX 94-02-A: defense-in-depth for compacted rows reaching FJMS)
  - serialize_search_payload result_domains kwarg short-circuit (MUST-FIX 94-02-A: bypass FJMS round-trip when caller has fresh domains)
  - _to_parallels_envelope_item D-10 strip + regression test pin (parallels JSON envelope shape unaffected)
  - export_json end-to-end live for has_pgp/is_printed
  - export_excel scope-staged reads with TODO(Wave 3) for the xlsx kwarg-pass
  - history-restore branch flagged with 'metadata_incomplete_restored_from_history' warning marker (SHOULD-FIX 94-02-C)
affects: [Wave 3 web xlsx restructure, Wave 4 desktop xlsx parity]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Opt-in per-item additive keys: _serialize_item omits keys entirely when caller passes neither set (preserves /api/search public shape) but emits both as booleans when either is passed (preserves D-06 always-boolean contract). MUST-FIX 94-02-B."
    - "Independent-field patch helper: update_search_export_enrichment patches each of 3 fields only when not None (passing None preserves existing value) — symmetric with Phase 88 update_search_export_selection."
    - "Shared sys_id resolver _extract_sys_id_for_batch — single source of truth for the 4-tier fallback chain (top-level sys_id → display.id → raw_header regex → uid + meta_mgr); both the per-item serializer and the batch builder use it to avoid drift."
    - "Caller-supplied data short-circuit: serialize_search_payload's result_domains kwarg bypasses FJMS batch lookup; session-payload export path has up-to-date data already."
    - "Defense-in-depth strip: _to_parallels_envelope_item .pop() the additive keys even when opt-in semantics already prevent their emission — pins the negative invariant against future regressions."
    - "Restored-snapshot warning marker: 'metadata_incomplete_restored_from_history' inserted into warnings list signals to JSON consumers that has_pgp/is_printed/domains are not authoritative on a snapshot replay."

key-files:
  created:
    - tests/test_export_state_enrichment.py
    - tests/test_parallels_envelope_no_pgp_keys.py
  modified:
    - web/export_state.py
    - web/pages/search.py
    - shared/search_serializer.py
    - web/api.py
    - tests/test_search_serializer.py

key-decisions:
  - "MUST-FIX 94-02-A applied: _extract_sys_id_for_batch shared resolver covers all 4 fallback paths (sys_id → display.id → raw_header regex → uid+meta_mgr); serialize_search_payload's batch builder now uses it, so compacted rows reach FJMS."
  - "MUST-FIX 94-02-A applied: serialize_search_payload accepts result_domains kwarg — when provided, FJMS round-trip is short-circuited (build domain_batch from the kwarg dict in the {sys_id: [{'domain': str, ...}]} shape _serialize_item consumes); /api/search path (no session) still uses FJMS lookup."
  - "MUST-FIX 94-02-B applied: _serialize_item emits has_pgp/is_printed ONLY when at least one of transcription_sys_ids/printed_sys_ids is provided (export path). The public /api/search caller at web/search_api.py:939-950 passes neither, so the response shape is unchanged (D-11 preserved)."
  - "SHOULD-FIX 94-02-C applied: history-restore branch in web/pages/search.py prepends 'metadata_incomplete_restored_from_history' to the warnings list — restored snapshots are flagged so export consumers know flags are not authoritative."
  - "SHOULD-FIX 94-02-D applied: export_excel reads the 3 enrichment fields INTO LOCAL VARIABLES IN SCOPE with TODO(Wave 3) marker — defers the kwarg-pass to Wave 3 when export_search_results_excel's signature is restructured (avoids a no-op stub)."
  - "D-10 / Q6 Codex preflight: _to_parallels_envelope_item explicitly strips has_pgp/is_printed even with opt-in semantics; the regression test tests/test_parallels_envelope_no_pgp_keys.py pins the negative invariant."
  - "Q3 Codex preflight: restored exports include the 'metadata_incomplete_restored_from_history' marker; the export consumer can branch on its presence."
  - "Q5 Codex preflight: Stage-1 + Stage-2 update_search_export_enrichment calls both fire — early exports between stages contain correct flags for visible-page sys_ids; Stage-2 re-sync covers off-page rows. Documented as acceptable graceful degradation in the threat model (T-94-19 disposition: accept)."
  - "Q7 Codex preflight: opt-in semantics close the public-API leak. /api/search consumers' response shape unchanged."
  - "Stage-2 enrichment update fires AFTER the chunk for-loop completes — single write covers all enriched data; avoids N writes inside the loop."
  - "JSON envelope schema_version stays 1 (Phase 83 additive-change commitment) — the new per-item keys are additive when present and absent when not, both compatible with skill consumers that ignore unknown keys or branch on presence."
  - "Phase 87 invariant preserved — zero raw app.storage.user.* accesses introduced; tests/test_no_raw_storage_access.py allowlist [] unchanged."

requirements-completed: [EXPORT-META-06, EXPORT-META-07]

# Metrics
duration: ~30min
completed: 2026-05-20
---

# Phase 94 Plan 02: Wave 2 — State Plumbing + JSON Envelope Extension Summary

**Plumbed the 3 enrichment signals (`transcription_sys_ids`, `printed_ids`, `result_domains`) through the web session payload via Phase-87-compliant safe_storage chokepoint helpers, and extended the per-item JSON envelope with opt-in `has_pgp` / `is_printed` boolean flags — preserving the public `/api/search` response shape per D-11 while activating the export-path enrichment per D-06.**

## Performance

- **Duration:** ~30 min
- **Tasks:** 4 (each committed atomically)
- **Files created:** 2 (`tests/test_export_state_enrichment.py`, `tests/test_parallels_envelope_no_pgp_keys.py`)
- **Files modified:** 5 (`web/export_state.py`, `web/pages/search.py`, `shared/search_serializer.py`, `web/api.py`, `tests/test_search_serializer.py`)
- **Net tests added:** 22 (9 export_state_enrichment + 2 parallels_no_pgp_keys + 11 TestSerializeItemEnrichmentFlags)
- **Test count delta:** 121 (Wave 1 close-out baseline) → 143+ Wave 2 close (no test was deleted; some are auto-collected through cross-cutting fixtures).
- **Net production lines added:** ~100 across 4 files (the bulk is search_serializer's new helper + the opt-in branch + the result_domains short-circuit).

## Accomplishments

- Extended `web/export_state.set_search_export(...)` with 3 new optional kwargs (`transcription_sys_ids`, `printed_ids`, `result_domains`) — sets cast to sorted lists for JSON-safety, dict copied for safe storage round-trip. Added new sibling helper `update_search_export_enrichment(...)` mirroring `update_search_export_selection`'s D-11 isinstance guard + D-12 copy-on-update conventions; each field is patched independently (None = leave alone).
- Wired 5 call sites in `web/pages/search.py`: 3 existing `set_search_export` calls (history-restore, partial-results, initial-completion) now pass empty enrichment containers, and 2 new `update_search_export_enrichment` calls fire post-Stage-1 visible-page enrichment and post-Stage-2 background-chunk-loop completion (both inside the generation-check gate). History-restore branch flags the restored snapshot via `'metadata_incomplete_restored_from_history'` warning marker.
- Extended `shared/search_serializer._serialize_item` with optional `transcription_sys_ids` / `printed_sys_ids` kwargs implementing MUST-FIX 94-02-B opt-in semantics: keys OMITTED when both None (preserves `/api/search` public shape), BOTH emitted as booleans when either provided (preserves D-06 always-boolean contract).
- Added MUST-FIX 94-02-A defense-in-depth: new module-level helper `_extract_sys_id_for_batch(result, meta_mgr=None) -> str` shared between the per-item serializer's SEED-002 fallback and the batch sys_id builder in `serialize_search_payload`. Compacted rows post-`_compact_search_result_row` now reach FJMS.
- Added MUST-FIX 94-02-A shipping fix: `serialize_search_payload` accepts a new `result_domains` kwarg — when provided, FJMS round-trip is short-circuited. The session-payload export path has up-to-date domains; the kwarg-pass at `web/api.py:export_json` bypasses the FJMS call entirely.
- `_to_parallels_envelope_item` strips `has_pgp`/`is_printed` (D-10 negative invariant) — defense-in-depth even with opt-in semantics, pinned by new regression test `tests/test_parallels_envelope_no_pgp_keys.py`.
- `web/api.py:export_json` is fully wired: passes `transcription_sys_ids` + `printed_sys_ids` + `result_domains` kwargs to `serialize_search_payload`. JSON endpoint now returns per-item `has_pgp` / `is_printed` flags AND uses the session payload's up-to-date domain data.
- `web/api.py:export_excel` reads the 3 fields INTO LOCAL VARIABLES IN SCOPE with a `TODO(Wave 3)` marker — defers the kwarg-pass to Wave 3 when `export_search_results_excel`'s signature is restructured.

## Task Commits

1. **Task 1: extend set_search_export + add update_search_export_enrichment helper** — `98ecfd3b` (feat)
   - 9 new tests in `tests/test_export_state_enrichment.py` covering backward-compat, round-trip with sets/lists, None defaults, isinstance guard, copy-on-update, all-None no-op, clear-via-empty, multitenant session isolation.
2. **Task 2: wire 5 search.py call sites** — `327096a1` (feat)
   - 3 existing `set_search_export` calls pass `transcription_sys_ids=set()`, `printed_ids=set()`, `result_domains={}`; 2 new `update_search_export_enrichment` post-Stage-1 + post-Stage-2 calls inside the generation-check gate; history-restore branch prepends `'metadata_incomplete_restored_from_history'` to warnings.
3. **Task 3: add has_pgp/is_printed + result_domains short-circuit + sys_id batch resolver** — `22d172e5` (feat)
   - 11 new tests in `TestSerializeItemEnrichmentFlags`: boolean semantics, D-11 opt-in omit, MUST-FIX 94-02-A short-circuit, batch builder compacted-row coverage. 2 new tests in `tests/test_parallels_envelope_no_pgp_keys.py` pinning the D-10 negative invariant.
4. **Task 4: wire web/api.py export_excel + export_json** — `33c5af68` (feat)
   - `export_json` fully functional in Wave 2 (returns `has_pgp` / `is_printed` per item AND uses session-payload domains). `export_excel` reads-only with TODO(Wave 3).

## Files Created/Modified

### Created
- `tests/test_export_state_enrichment.py` (~140 lines) — 9 tests covering set_search_export 3 new kwargs + update_search_export_enrichment behavior.
- `tests/test_parallels_envelope_no_pgp_keys.py` (~80 lines) — 2 tests pinning D-10 negative invariant.

### Modified
- `web/export_state.py` — `set_search_export` gains 3 new optional kwargs (Optional[Any] / Optional[Dict]); body casts sets to sorted lists for JSON-safety, dict copied. New `update_search_export_enrichment(...)` sibling helper preserves Phase 88 D-11 isinstance guard + D-12 copy-on-update; each kwarg is patched independently.
- `web/pages/search.py` — 5 call sites: 3 existing `set_search_export` calls extended (history-restore, partial-results, initial-completion); 2 new `update_search_export_enrichment` calls (post-Stage-1, post-Stage-2). History-restore prepends `'metadata_incomplete_restored_from_history'` to warnings.
- `shared/search_serializer.py` — added `_SYS_ID_REGEX` module-level compiled pattern; new `_extract_sys_id_for_batch(result, meta_mgr=None)` helper; `_serialize_item` extended with `transcription_sys_ids` / `printed_sys_ids` optional kwargs implementing MUST-FIX 94-02-B opt-in semantics; `serialize_search_payload` extended with 3 new kwargs (`transcription_sys_ids`, `printed_sys_ids`, `result_domains`) + result_domains short-circuit + sys_id batch builder swap to shared helper; `_to_parallels_envelope_item` strips `has_pgp`/`is_printed` (D-10) post `_serialize_item` call.
- `web/api.py` — `export_excel` extracts 3 new fields into local variables with `TODO(Wave 3)` marker; `export_json` passes 3 new kwargs to `serialize_search_payload` (fully functional).
- `tests/test_search_serializer.py` — new `TestSerializeItemEnrichmentFlags` class with 11 tests (boolean semantics, D-11 opt-in omit, MUST-FIX 94-02-A short-circuit + batch builder).

## Codex Pre-flight Critique Folded In

| ID | Pin |
|---|---|
| **Q1 / Q2** — domain_batch builder must use shared resolver for compacted rows | `_extract_sys_id_for_batch` helper added; `serialize_search_payload`'s batch builder uses it; `test_serialize_search_payload_batch_builder_covers_compacted_rows` pins compacted-row coverage. |
| **Q3** — history-restore must mark restored exports as metadata-incomplete | `'metadata_incomplete_restored_from_history'` warning marker prepended in `web/pages/search.py:3902`. |
| **Q4** — wording "reads + passes" → "reads into local variables only" for export_excel | `export_excel` reads-only with `TODO(Wave 3)` marker; ruff + tests green. |
| **Q5** — partial enrichment between stages | Stage-1 + Stage-2 calls both fire; threat model T-94-19 documents acceptance + future smoke checklist. |
| **Q6 (OK)** — parallels D-10 strip + regression test | `_to_parallels_envelope_item` strips both keys; `tests/test_parallels_envelope_no_pgp_keys.py` pins negative invariant. |
| **Q7** — public /api/search opt-in semantics | `_serialize_item` omits keys when both kwargs None; `test_flags_omitted_when_kwargs_none` pins it; `web/search_api.py` unchanged (no kwargs passed → no key leak). |

## D-11 Public-API Stability Confirmation

The public `/api/search` endpoint at `web/search_api.py:939-950` calls `serialize_search_payload(...)` WITHOUT passing `transcription_sys_ids` or `printed_sys_ids` kwargs. With the MUST-FIX 94-02-B opt-in semantics:

- `_serialize_item` receives `transcription_sys_ids=None` and `printed_sys_ids=None` (defaults).
- The `_emit_enrichment_flags` flag is `False` (both kwargs are None).
- The returned dict does NOT contain `has_pgp` or `is_printed` keys at all.

Pinned by `test_flags_omitted_when_kwargs_none`. The public response shape is unchanged.

## D-06 Always-Boolean Contract Confirmation

When `web/api.py:export_json` passes either set kwarg to `serialize_search_payload` (export path), `_serialize_item` receives at least one non-None set:

- `_emit_enrichment_flags` is `True`.
- BOTH `has_pgp` and `is_printed` are emitted as booleans, never None, never missing.
- Empty sys_id yields `False` (not None) for both — pinned by `test_empty_sys_id_with_kwargs_provided`.

## D-10 Parallels Envelope Confirmation

`_to_parallels_envelope_item` calls `_serialize_item` WITHOUT the 2 enrichment kwargs, AND explicitly `.pop()`s both keys before returning. The parallels JSON envelope shape via `/api/parallels` and `/api/export/parallels/json` is unaffected. Pinned by `tests/test_parallels_envelope_no_pgp_keys.py`.

## Multitenant Invariant Confirmation

Zero raw `app.storage.user.*` accesses introduced under `web/`. All payload writes route through `web/safe_storage.py` chokepoint via:
- `set_search_export` → `safe_user_set(_SEARCH_KEY, ...)` (existing chokepoint)
- `update_search_export_enrichment` → `safe_user_get(_SEARCH_KEY, None)` + `safe_user_set(_SEARCH_KEY, ...)` (new chokepoint use, follows Phase 88 D-11/D-12 conventions)

Verified by:
```
$ python -m pytest tests/test_no_raw_storage_access.py -q
6 passed
```
Phase 87 allowlist remains `[]`. No new entries needed.

## Decisions Made

- The `_to_parallels_envelope_item` strip via `.pop(has_pgp, None)` is RETAINED even though MUST-FIX 94-02-B opt-in semantics ALREADY prevent the keys from being emitted on the parallels path (the parallels builder never passes the kwargs). This is intentional defense-in-depth: a future caller might evolve to pass the kwargs through, and the strip ensures the negative invariant holds regardless. The regression test pins both the omit semantics AND the strip behavior.
- `result_domains` short-circuit converts the simpler `{sys_id: [str]}` kwarg shape to the `_serialize_item`-consumed `{sys_id: [{'domain': str}]}` shape inline at the call site, rather than changing the `_serialize_item` contract. Rationale: keep the per-item serializer's contract narrow; the short-circuit is a `serialize_search_payload`-level concern.
- The Stage-2 update call fires AFTER the for-loop completes (not inside it) to avoid N writes inside the chunk loop. The single write covers all enriched data — exports between Stage-1 and Stage-2 completion contain partial enrichment for visible-page rows, accepted as graceful degradation per threat model T-94-19.
- Local-import-inside-function pattern for `update_search_export_enrichment` matches the existing convention at all 5 sites (3 `set_search_export` + 2 new) — keeps the import overhead at the call site rather than module-load time.

## Deviations from Plan

**None of substance** — plan executed essentially as written. Three minor deviations applied automatically (no permission needed):

- The MUST-FIX 94-02-A test `test_result_domains_kwarg_short_circuits_fjms` checks the output domain shape with both forms (dict with `'domain'` or `'name'` key, OR plain str). The actual production path emits dicts with `'domain'` key per the existing FJMS shape; the test's `flat` extraction handles either form for forward-compatibility.
- The MUST-FIX 94-02-A test `test_serialize_search_payload_batch_builder_covers_compacted_rows` uses `monkeypatch.setattr(search_serializer, '_safe_fjms_lookups', _capture)` to verify the batch builder receives both live and compacted row sys_ids. The compacted row's `raw_header` contains a synthetic 99-prefix string `99001234567B` which contains digits-then-B (passes the regex `r'(99\d{8,})'` because there are 11 digits before the `B`).
- The history-restore branch comment was slightly expanded with the rationale for the warning marker (SHOULD-FIX 94-02-C wording from the plan).

## Wave 2 → Wave 3 Hand-off

Wave 3's scope (per `94-CONTEXT.md` D-15):

- Restructure `web/export_service.py:export_search_results_excel` to emit a 3-sheet workbook consuming `shared/export_dossier.py` (4 lookup helpers + 2 row emitters + 2 header constants, all shipped in Wave 1).
- Switch to unified column order per D-01 (System ID | Library | Shelfmark | Title | Image/Page | Source | Snippet | Full Text | Has PGP | Is Printed | Domains | IIIF Manifest).
- Extract `Image/Page` + `Source` from web result dict — `display.img` and `display.source` per 94-PATTERNS.md.
- Extend rich-text snippet rendering to web (D-14) via the `shared_export_utils.build_rich_snippet_cell` helper shipped in Wave 1.
- Wire conditional RTL (D-04) — Hebrew UI → RTL on all 3 sheets.
- CLOSE the `TODO(Wave 3)` in `web/api.py:export_excel` by passing the 3 enrichment kwargs through to the restructured `export_search_results_excel` signature.

Wave 3 will consume Wave 2's session payload via the existing `get_search_export()` reader — no further state plumbing changes needed.

## Self-Check: PASSED

**Created files verified to exist:**

- `tests/test_export_state_enrichment.py` — FOUND
- `tests/test_parallels_envelope_no_pgp_keys.py` — FOUND

**Modified files verified to contain new symbols:**

- `web/export_state.py` — contains `def update_search_export_enrichment`, `transcription_sys_ids`, `printed_ids`, `result_domains`
- `web/pages/search.py` — contains `update_search_export_enrichment`, `metadata_incomplete_restored_from_history`, `transcription_sys_ids=set()` (3x), `transcription_sys_ids=search_state.transcription_sys_ids` (2x)
- `shared/search_serializer.py` — contains `_extract_sys_id_for_batch`, `has_pgp`, `is_printed`, `transcription_sys_ids`, `printed_sys_ids`, `result_domains`
- `web/api.py` — contains `transcription_sys_ids=set(`, `printed_sys_ids=set(`, `result_domains=session_payload.get`, `TODO(Wave 3`
- `tests/test_search_serializer.py` — contains `TestSerializeItemEnrichmentFlags`

**Commits verified to exist in `git log --oneline`:**

- `98ecfd3b` (feat(94-02): extend set_search_export with 3 enrichment kwargs + update_search_export_enrichment helper) — FOUND
- `327096a1` (feat(94-02): wire 5 set_search_export/update_search_export_enrichment call sites in search.py) — FOUND
- `22d172e5` (feat(94-02): add has_pgp/is_printed per-item flags + result_domains short-circuit + sys_id batch resolver) — FOUND
- `33c5af68` (feat(94-02): wire export_excel + export_json to thread enrichment kwargs) — FOUND

**Verification commands run green:**

- `python -m pytest tests/test_export_state_enrichment.py tests/test_export_state_selection.py tests/test_export_state_cap.py tests/test_search_serializer.py tests/test_parallels_envelope_no_pgp_keys.py tests/test_no_raw_storage_access.py tests/test_export_dossier.py tests/test_shared_rich_snippet.py tests/test_api_export_json.py tests/test_export_cross_user_isolation.py tests/test_api_legacy_unchanged.py -q` → 186 passed
- `python -c "from shared.search_serializer import SCHEMA_VERSION; assert SCHEMA_VERSION == 1; print('OK')"` → OK
- `python -m ruff check web/export_state.py web/pages/search.py web/api.py shared/search_serializer.py tests/test_export_state_enrichment.py tests/test_parallels_envelope_no_pgp_keys.py` → All checks passed
- `grep "app\.storage\.user\." web/export_state.py web/pages/search.py web/api.py shared/search_serializer.py` → no output (Phase 87 invariant preserved)
- `python -c "import ast; ast.parse(open('web/pages/search.py', encoding='utf-8').read())"` → syntax OK
- `python -c "import ast; ast.parse(open('web/api.py', encoding='utf-8').read())"` → syntax OK
