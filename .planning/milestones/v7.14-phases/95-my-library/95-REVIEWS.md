---
phase: 95
reviewers: [codex]
reviewed_at: 2026-05-21
plans_reviewed:
  - 95-01-wave0-stubs-and-packaging-PLAN.md
  - 95-02-local-sys-id-and-parser-PLAN.md
  - 95-03-local-indexer-core-PLAN.md
  - 95-04-cloud-write-gates-PLAN.md
  - 95-05-main-search-merger-rrf-PLAN.md
  - 95-06-lab-side-index-and-invalidation-PLAN.md
  - 95-07-my-library-tab-PLAN.md
  - 95-08-result-badge-and-filter-PLAN.md
  - 95-09-docs-export-web-guard-and-packaging-PLAN.md
context_artifacts:
  - 95-SPEC.md
  - 95-CONTEXT.md (46 decisions, post-internal-Codex-critique)
  - 95-RESEARCH.md
  - 95-PATTERNS.md
  - 95-VALIDATION.md (nyquist_compliant: true)
review_pass: 3
prior_passes:
  - 1: Internal Codex critique on CONTEXT.md (surfaced 2 P0s + 8 gaps → D-34..D-46)
  - 2: Internal gsd-plan-checker (2 blockers + 8 warnings resolved across iterations 1+2)
---

# Cross-AI Plan Review — Phase 95

## Codex Review (gpt-5.5)

### Summary

The plan set is unusually thorough and has clearly absorbed the earlier critiques, especially around RRF, post-dedup merge ordering, raw-tokenized `unique_id`, and top-of-function cloud gates. I would not treat it as implementation-ready yet. I see several remaining blockers around same-session index visibility, delete atomicity, `lists_sync` fallback behavior, and LOCAL LAB build feasibility. These are not polish issues; a few can prevent the phase from meeting its core goal or can still leak cloud activity.

### Strengths

- The namespace model is clean: `97` prefix, helper-only checks, parser compatibility, and `LIBRARY_CODES` integration are well-covered.
- The previous P0s are addressed explicitly: RRF post-`_deduplicate()` and early `lists_sync` gates are prominent.
- Raw tokenizer on LOCAL `unique_id` is correctly called out as load-bearing for delete-by-term.
- Validation coverage is broad, with targeted tests for dedup ordering, cloud gates, schema evolution, fallback behavior, filter cascade, and packaging.
- UI lifecycle risks are recognized: QThread, cancellation, QMutex, unavailable folders, and aggregate scale warnings are all planned.

### Concerns

- **HIGH — LOCAL index refresh is not wired back into the live search engine.** Plan 05 opens `self.local_searcher` only during `SearchEngine` initialization (95-05 lines 178, 184). Plan 07 runs refresh workers, but does not specify reloading/reopening `local_searcher` or `local_lab_searcher` after commits (95-07 line 295). Result: newly indexed files may not appear until restart, breaking the main product promise.

- **HIGH — `sync_item_to_cloud` still leaks if `item_data` is missing and `item_id` itself is a LOCAL sys_id.** The planned gate only checks `is_local_sys_id` inside `if item_data:` (95-04 line 225). The missing-item test explicitly allows proceeding to existing flow (95-04 line 215). The gate should compute `sys_id = item_data.get('sys_id', item_id) if item_data else item_id` before any cloud call.

- **HIGH — delete/removal is not crash-safe.** The two-phase protocol covers `pending` inserts, but `_delete_file` deletes SQLite rows before confirming Tantivy delete commit (95-03 lines 377, 378). A crash after SQLite DELETE but before Tantivy commit leaves orphaned searchable LOCAL docs with no `local_pages` rows to delete later. The earlier `pending_delete` decision is not actually implemented.

- **HIGH — LOCAL LAB build has no defined content source.** Plan 06 says to iterate `local_files` and yield `content` (95-06 lines 201, 203), but Plan 03's SQLite schema stores metadata only, not page text (95-03 lines 125, 126). The plan must specify whether LAB rebuild reads stored Tantivy docs, re-extracts files, or stores normalized page text.

- **HIGH — the packaging "smoke" does not test the packaged EXE despite claiming it does.** The must-have says the smoke runs against `dist/GenizahSearchPro.exe` (95-09 lines 29, 72). The test text later admits it only imports `fitz` in the venv and checks the `.spec` (95-09 line 116). This will not catch the `fitz._fitz` packaged-binary failure D-43 is meant to catch.

- **MEDIUM — local search query semantics are underspecified and may diverge from main search.** `_query_local_index` uses a fresh `parse_query` against `content`, `content_head`, and `content_tail` (95-05 lines 203, 205). The plan does not prove phrase mode, gap mode, exclusions, Responsa syntax, or Hebrew expansion are identical between main and LOCAL search.

- **MEDIUM — TXT decoding silently corrupts non-UTF-8 input.** The plan locks `utf-8-sig` but then uses `errors="replace"` (95-03 lines 498, 500). That means cp1255 or damaged input becomes indexed garbage instead of `encoding_error`.

- **MEDIUM — namespace regression does not scan the full `libraries.csv`.** The SPEC requires checking every row, but the planned test stops after 1,000 rows when no offender is found (95-02 line 374). This weakens the locked acceptance criterion for little runtime savings.

- **LOW — Wave 0 "red stubs" are partly skips, then `wave_0_complete` is set true.** The plan calls skips "red" (95-01 lines 390, 392) and marks validation complete immediately (95-01 line 467). This is process confusion more than product risk.

### Suggestions

- Add `SearchEngine.reload_local_indexes()` / `reload_local_lab_index()` and call them after MyLibraryTab refresh, delete, rebuild, and recovery.
- Implement true delete two-phase: `pending_delete` rows, Tantivy delete commit, then SQLite deletion; recover `pending_delete` on startup.
- Fix `sync_item_to_cloud` to gate on `item_id` when `item_data` is absent, and add a test for `item_id="970..."` with missing item data.
- Define LOCAL LAB source rows explicitly: read content from LOCAL Tantivy by UID or re-extract from file paths; then test LAB rebuild after app restart.
- Replace local query duplication with a shared query-builder path from the main search code.
- Use strict TXT decoding with explicit `encoding_error`, or add deliberate cp1255 fallback; do not index replacement-character output silently.
- Make the packaging test actually launch the built EXE or add a release-gated subprocess smoke that exercises PyMuPDF inside `dist`.
- Run the full `libraries.csv` namespace scan; 255K rows is acceptable for this invariant.

### Risk Assessment

**Overall risk: HIGH** until the blockers above are patched. The plans are strong architecturally, but current gaps can cause same-session LOCAL search to fail, orphan private docs in the index after crashy deletes, allow a cloud client touch through a missing-item `lists_sync` path, and leave Composition/LAB integration nonfunctional. Once those are fixed, the remaining risk drops to MEDIUM, mostly around UI complexity and PDF extraction quality.

---

## Consensus Summary

> Only one external reviewer (Codex) was invoked for this review pass — there is no cross-reviewer consensus to extract. The findings below summarize Codex's verdict.

### Top Findings (HIGH severity — must address before execution)

1. **Live-search reload after refresh** (Plan 05/07) — `SearchEngine.reload_local_indexes()` missing; newly indexed files won't appear until app restart.
2. **`sync_item_to_cloud` LOCAL gate gap** (Plan 04) — missing-`item_data` path still calls `_get_client()` if `item_id` itself is LOCAL.
3. **Delete is not crash-safe** (Plan 03) — SQLite DELETE precedes Tantivy commit; `pending_delete` two-phase recovery decided in D-21 but not implemented.
4. **LOCAL LAB content source undefined** (Plan 06 vs Plan 03) — Plan 06 reads `content` from `local_files`, but Plan 03's schema stores metadata only (no page text). LAB rebuild after restart cannot work as planned.
5. **Packaging smoke doesn't run packaged EXE** (Plan 09) — D-43's whole point (catching `fitz._fitz` binary collection failures) is bypassed by importing `fitz` in the venv.

### MEDIUM findings (should address)

- Local search query semantics may diverge from main search (phrase mode, gap mode, exclusions, Responsa syntax, Hebrew expansion).
- TXT `errors="replace"` silently corrupts non-UTF-8 input instead of marking `encoding_error`.
- `libraries.csv` namespace regression test stops at 1,000 rows; SPEC requires every row.

### Divergent Views

N/A — single reviewer.

### Recommendation

Replan via `/gsd-plan-phase 95 --reviews` to fold these 5 HIGH findings into the affected plans (mainly 03, 04, 05, 06, 07, 09). The 3 MEDIUM findings can be folded in the same pass or deferred to execution with explicit acceptance-test additions. The 1 LOW is process hygiene only.
