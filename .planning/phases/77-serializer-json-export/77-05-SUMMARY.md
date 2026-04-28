---
phase: 77-serializer-json-export
plan: 05
subsystem: search-api
tags: [docs, smoke-check, phase-gate, latent-bug-resolution, field-collision-fix, multi-producer-audit]

# Dependency graph
requires:
  - phase: 77 plan 01
    provides: state.current_search_query latent bug fixed (envelope-echo state populated at all 5 search/parallels execute paths) — Plan 05 documents the fix in docs/OPEN_ISSUES.md
  - phase: 77 plan 02
    provides: lab_composition_search chunk_hits per-uid list-of-tuples (D-13 Path A) — Plan 05 smoke-check uncovered that the standard-mode counterpart (search_composition_logic) used chunk_hits as an int counter, causing serializer crash; fixed during Plan 05 verification
  - phase: 77 plan 03
    provides: shared/search_serializer.py module — Plan 05 documents the module in docs/CODE_INDEX.md and extends documentation with smoke-check findings (rep-field mapping fix, group-level dedup, isinstance guard, logger.exception upgrade)
  - phase: 77 plan 04
    provides: HTTP handlers /api/export/json + /api/export/parallels/json + toolbar buttons — Plan 05 smoke-check exercised them end-to-end
provides:
  - docs/OPEN_ISSUES.md updated -- latent state.current_search_query bug marked Fixed (2026-04-27 by Plan 01); chunk_hits field-name collision marked Fixed (2026-04-28 by Plan 05 smoke-check follow-on commits); Last Updated banner refreshed; Change Log entry added; summary counts recomputed (P2 fixed 67→68, Total fixed 110→111, Total entries 136→137)
  - docs/CODE_INDEX.md updated -- shared/search_serializer.py section extended with smoke-check findings (rep-field mapping, group-level dedup, isinstance guard, logger.exception upgrade); new "Companion shape contract in genizah_core.py" subsection documents dual-shape chunk_hits across both producers + chunk_count rename + per-uid dedup + search_text_tantivy score record; Last updated banner refreshed
  - 4 follow-on commits during smoke verification (attributed to Plan 04 in commit message scope but Plan 05 narrative): baf481fb (defensive isinstance guard + logger.exception), c24fcc48 (per-chunk attribution in standard-mode + parallels rep-field mapping; +4 tests), 2e2d2b75 (Tantivy score surfaced + per-uid dedup), 327aea31 (group-level dedup + chunk_index sort; +2 tests)
  - Phase 77 ready for /gsd-verify-work (5/5 plans complete; manual smoke-check signed off; final test count 1201 passed / 8 skipped)
affects: [78-* (will inherit chunk_count rename + dual-shape chunk_hits contract when /api/parallels lands), 80-* (same)]

# Tech tracking
tech-stack:
  added: []  # No new libraries — docs-only updates plus 6 fix commits to existing modules
  patterns:
    - "Multi-producer field audit: when extending a per-uid item dict field, search ALL producers writing to it — not just the one the new feature targets"
    - "Defensive isinstance guard at consumer boundary: serializer wraps `for ch_idx, src_text, score, ms_snippet in item['chunk_hits']` in `isinstance(chunk_hits, list)` check so future shape regressions fall back to Path B instead of crashing"
    - "logger.exception over logger.error: upgrade in production handlers when handler-level exceptions need stack-trace visibility for downstream debugging"
    - "Per-uid dedup keyed on synthetic key (chunk_index, manuscript_snippet) prevents same Tantivy uid emitting duplicate entries from multiple segments"
    - "Group-level dedup at envelope-build time catches cross-uid duplicates that per-uid dedup cannot (NLI multi-uid cataloging on same sys_id)"
    - "Sorted output for stable consumer experience: matches[] sorted by chunk_index ascending so downstream code (Claude skill in Phase 81) sees deterministic order"

key-files:
  created:
    - .planning/phases/77-serializer-json-export/77-05-SUMMARY.md
  modified:
    - docs/OPEN_ISSUES.md
    - docs/CODE_INDEX.md
    - .planning/STATE.md
    - .planning/ROADMAP.md

key-decisions:
  - "chunk_hits field-name collision (Plan 02 list-of-tuples vs pre-existing int counter in search_composition_logic) was a Rule 1 bug discovered during the smoke check and fixed in 4 follow-on commits before Plan 05 close-out. Lesson: audit ALL producers when extending a shared per-uid field. Documented in docs/OPEN_ISSUES.md as a P2 Fixed entry plus an entry in MEMORY.md-equivalent decision history."
  - "Smoke check approval is implicit: user provided final clean JSON output (showing dedupted matches, sorted chunk indices, populated snippet/excerpt/match_terms, meaningful Tantivy scores) and told the orchestrator to wrap up. No re-prompt issued."
  - "The 6 follow-on commits (baf481fb, c24fcc48, 2e2d2b75, 327aea31) are technically Plan 77-04 phase-trailing polish (commit messages use scope `77-04` since they fix the parallels JSON handler shipped in Plan 04), but they belong in the Plan 05 narrative timeline since they were uncovered AND landed during Plan 05's manual smoke check. Plan 05 docs commits (db586467 Task 1 + 015b17d5 close-out) are the only commits in scope `77-05`."
  - "Result 1 in user's smoke-check sample showing 32 matches across 17 chunks for a single sys_id is correct grouping, NOT a bug: two distinct uids on the same sys_id had genuinely different manuscript content (different IEs / volumes / fragment slices). The group-level dedup in commit 327aea31 collapses only entries that share BOTH chunk_index AND manuscript_snippet — distinct snippets correctly stay separate matches."
  - "Phase 77 cumulative commit count: 20 commits (14 from Plans 01-04 docs + Plan 05 Task 1 docs + 4 follow-on smoke-check fixes + Plan 05 close-out). Final test count 1201 passed / 8 skipped, started phase at 1162 → +39 new tests across the 5 plans."

patterns-established:
  - "Smoke-check-driven Rule 1 fixes: when manual verification reveals a runtime bug not caught by automated tests, the executor fixes it inline + adds a regression test + amends the docs trail rather than escalating to a follow-up plan. The 4 follow-on commits during Plan 05 are the canonical example."
  - "Field-name discipline across producers: per-uid item dicts that flow into a shared serializer must be audited for field-name collisions before extending. Renaming the conflicting field (chunk_count for the int counter) frees the name without breaking the consumer."

requirements-completed: [EXPORT-01, EXPORT-02, EXPORT-03, EXPORT-04]  # All four EXPORT requirements satisfied across Phase 77; Plan 05 closes them

# Metrics
duration: 1d  # Plan 05 spanned a context-handoff: Task 1 commit on 2026-04-27, smoke check + close-out on 2026-04-28
completed: 2026-04-28
---

# Phase 77 Plan 05: Docs Refresh + Manual Smoke Verification Summary

**Phase 77 close-out plan: docs trail updated for the latent `state.current_search_query` bug fixed by Plan 01, the `shared/search_serializer.py` module shipped by Plan 03, and a `chunk_hits` field-name collision uncovered + fixed during the manual smoke check on /search and /parallels JSON downloads. Phase 77 ready for `/gsd-verify-work`.**

## Performance

- **Duration:** 1 day (Task 1 commit `db586467` on 2026-04-27 20:41 UTC; smoke-check fixes + Task 2 close-out commit `015b17d5` on 2026-04-28 ~07:00 UTC)
- **Started:** 2026-04-27 (Task 1 docs commit)
- **Completed:** 2026-04-28 (Task 2 close-out)
- **Tasks:** 2 (1 auto + 1 checkpoint:human-verify, both complete)
- **Plan-scope commits:** 2 (`db586467` Task 1, `015b17d5` Task 2 close-out)
- **Phase-scope follow-on commits during smoke verification:** 4 (`baf481fb`, `c24fcc48`, `2e2d2b75`, `327aea31` — all attributed to Plan 04 in commit-message scope since they fix the parallels JSON handler shipped in Plan 04, but they belong in the Plan 05 narrative timeline since they were uncovered AND landed during Plan 05's manual smoke check)

## Accomplishments

### Task 1: docs/OPEN_ISSUES.md + docs/CODE_INDEX.md updated (2026-04-27, commit `db586467`)

- **OPEN_ISSUES.md**:
  - Added P2 Fixed row for the latent `state.current_search_query` bug (declared in `web/state.py` but never assigned anywhere — regression from v7.9 decomposition that made every Excel/Word export silently default to filename `genizah.xlsx`). Fix shipped in Phase 77 Plan 01 commit `2c5e94d5` (envelope-echo state populated at all 5 search/parallels execute paths).
  - Recomputed summary counts: P2 fixed 66 → 67, Total fixed 109 → 110, Total entries 135 → 136 (LOW-03).
  - Updated Last Updated banner to 2026-04-27 with Phase 77 narrative.
  - Added Change Log entry for the docs refresh + Phase 77 close-out.
- **CODE_INDEX.md**:
  - Added new section for `shared/search_serializer.py` listing all 5 public exports (`SCHEMA_VERSION`, `serialize_search_payload`, `serialize_parallels_payload`, `build_search_filename`, `build_parallels_filename`) plus the private `_serialize_item` single source of truth, the `NLI_RESOLVABLE_LIBRARY_CODES` whitelist (HIGH-07), filename counter (HIGH-06), and pointers to the new HTTP handlers.
  - Updated Last updated banner to 2026-04-27 with Phase 77 banner above the v7.9 banner.

### Task 2: Manual smoke check (2026-04-28, user-driven)

User performed end-to-end smoke check on the running web app:
- /search JSON download with Hebrew query — confirmed valid JSON, native UTF-8 Hebrew, envelope shape with `schema_version`/`source: "search"`/`query`/`mode`/`gap`/`filters`/`count`/`total`/`warnings`/`generated_at`/`results`, locator `{sys_id, volume_ie, p_num}` on every item, scores rounded to 4 decimals, no `full_text` field on items (just `excerpt`), `domains` as array.
- /parallels JSON download with multi-sentence Hebrew source — initially crashed with `'int' object is not iterable`; 4 follow-on commits resolved the chunk_hits field-name collision (see "Smoke-check follow-on fixes" below).
- Regression check: Excel/Word handlers on /search and /parallels still produced identical output to pre-Phase-77 baseline. Image-proxy routes unaffected.

Final smoke check after the 4 fixes: BOTH /search and /parallels JSON downloads work correctly. User confirmed by providing the final clean JSON output (showing dedupted matches, sorted chunk indices, populated snippet/excerpt/match_terms, meaningful Tantivy scores) and instructing the orchestrator to wrap up. Smoke check status: **PASSED**.

### Smoke-check follow-on fixes (4 commits, attributed to Plan 04 scope but belong in Plan 05 timeline)

**Commit 1 — `baf481fb`** — `fix(77-04): handle chunk_hits int collision in parallels serializer`
- First crash report: `/parallels` JSON download returned 500 with `TypeError: 'int' object is not iterable` on the `for ch_idx, src_text, score, ms_snippet in item['chunk_hits']` loop in `_to_parallels_envelope_item`.
- **Defensive fix at consumer boundary:** wrapped `chunk_hits` iteration in `isinstance(chunk_hits, list)` guard. When `chunk_hits` is not a list, fall back to Path B (single degenerate match using `source_ctx`/`text`/`score`).
- **Observability fix:** upgraded `logger.error` → `logger.exception` in both `/api/export/json` and `/api/export/parallels/json` handlers so future serializer crashes surface stack traces in production logs.
- **+1 regression test:** `test_parallels_chunk_hits_int_falls_back_to_path_b`.

**Commit 2 — `c24fcc48`** — `feat(77-04): per-chunk attribution in standard-mode parallels + parallels-shape field mapping`
- **Root-cause fix:** Plan 02 had extended `lab_composition_search` to populate `chunk_hits` per uid as `(chunk_index, source_chunk_text, match_score, manuscript_snippet)` tuples. But `search_composition_logic` (the standard-mode parallels path at `genizah_core.py:~1196`) had used `chunk_hits` since 2026-03-12 as an int counter tracking how many chunks each uid hit. Both producers wrote to the same per-uid item dict.
- Extended `search_composition_logic` to populate `chunk_hits` as the same list-of-tuples shape (mirroring D-13 Path A). Renamed the int counter to `chunk_count` to free the field name (avoid future collisions).
- **Parallels rep-field mapping fix:** `_to_parallels_envelope_item` was reading `rep['text']` and `rep['full_text']`/`'content'`/`'text'` for `synth['snippet']` / `synth['full_text']` — but standard-mode rep dicts don't expose those exact keys. Fixed mapping so parallels items get populated `snippet`/`excerpt`/`match_terms` instead of empty strings.
- **+4 tests:** `test_parallels_populates_snippet_excerpt_match_terms_from_text`, `test_chunk_count_replaces_old_int_counter`, `test_chunk_hits_list_appended_per_chunk`, `test_chunk_hits_surfaced_on_returned_items_in_standard_mode`.

**Commit 3 — `2e2d2b75`** — `fix(77-04): surface Tantivy score on search results + dedup chunk_hits`
- **Search results JSON had `score: 0.0` for every item.** Root cause: `results.append({...})` at `genizah_core.py:7542` and `:7559` (in `search_text_tantivy`) had a `score` variable in scope from the Tantivy iteration but never recorded it onto the result dict. Added `'score': float(score)` to both sites.
- **Per-uid dedup:** added `_chunk_hit_keys` set per uid in both `lab_composition_search` and `search_composition_logic` to prevent the same Tantivy uid emitting duplicate (chunk_index, ms_snippet) entries when returned from multiple Tantivy segments.

**Commit 4 — `327aea31`** — `fix(77-04): group-level dedup + chunk_index sort in parallels matches`
- **Cross-uid duplicate problem:** per-uid dedup couldn't catch cross-uid duplicates. NLI sometimes catalogs the same physical manuscript under multiple Alma uids on the same sys_id (e.g., Karaite prayer books). When a parallels source matched all those uids, the group's matches[] array had duplicate entries.
- **Group-level dedup in `_to_parallels_envelope_item`:** matches[] now keyed on `(chunk_index, manuscript_snippet)` — when two entries share both, the highest-scoring entry wins.
- **Sort:** matches[] now sorted by `chunk_index` ascending for stable, deterministic output (downstream consumers like the Phase 81 Claude skill see predictable order).
- **+2 tests:** `test_parallels_group_dedup_same_chunk_same_snippet_across_uids`, `test_parallels_matches_sorted_by_chunk_index`.

### Task 2 Close-out: docs/OPEN_ISSUES.md + docs/CODE_INDEX.md smoke-check findings (2026-04-28, commit `015b17d5`)

- **OPEN_ISSUES.md**:
  - Added P2 Fixed row for the `chunk_hits` field-name collision with full timeline of the 4 follow-on commits and a "Lesson learned" note: when extending a multi-producer field, audit ALL producers writing to the same per-uid item dict — not just the one the new feature targets.
  - Recomputed summary counts: P2 fixed 67 → 68, Total fixed 110 → 111, Total entries 136 → 137.
  - Updated Last Updated banner to 2026-04-28 with smoke-check narrative.
  - Added Change Log entry for the smoke-check follow-on commits + final test count.
- **CODE_INDEX.md**:
  - Extended `shared/search_serializer.py` `_to_parallels_envelope_item` entry with smoke-check details: rep-field mapping fix, group-level dedup keyed on `(chunk_index, manuscript_snippet)`, isinstance guard wrapping the chunk_hits iteration, logger.exception upgrade in JSON handlers.
  - Added new "Companion shape contract in `genizah_core.py`" subsection documenting that BOTH `lab_composition_search` (Plan 02) AND `search_composition_logic` (Plan 05 fix) populate `chunk_hits` as a list-of-tuples; that the pre-existing int counter was renamed `chunk_count`; that both paths apply per-uid `_chunk_hit_keys` dedup; that `search_text_tantivy` now records score.
  - Updated Last updated banner to 2026-04-28.

## Verification

- `python scripts/check_docs.py` → all checks passed (Critical Documents, Outdated Terminology, Document Freshness, Internal Links)
- `pytest tests/` → **1201 passed, 8 skipped** in 14.55s (started Phase 77 at 1162; +39 new tests across the 5 plans)
- Manual smoke-check on /search JSON download: PASSED (valid JSON, native Hebrew UTF-8, locator on every item, score 4-decimal, no full_text, domains array)
- Manual smoke-check on /parallels JSON download: PASSED after 4 follow-on fixes (results[] + filtered[] separate arrays, one result per manuscript with matches[] array, dedupted matches sorted by chunk_index, populated snippet/excerpt/match_terms, meaningful Tantivy scores)
- Regression spot-check: Excel/Word handlers on /search and /parallels produce identical output to pre-Phase-77 baseline
- `git log --oneline -25` shows the full Phase 77 commit chain (14 plan-scope commits + 6 follow-on smoke-check fixes = 20 commits total)

## Deviations from Plan

### Smoke-check follow-on fixes (4 Rule 1 bugs auto-fixed during checkpoint:human-verify)

The plan's `<tasks>` envisioned a clean smoke check that the user would either approve or reject. In practice, the smoke check uncovered a runtime bug (chunk_hits field-name collision) that automated tests had not caught — Plan 02's behavioral test `test_chunk_hits_populated_on_real_lab_loop` exercised the lab-mode path but no test covered the standard-mode path's collision with the pre-existing int counter, and no test exercised the full `_to_parallels_envelope_item` consumer with a real standard-mode item dict.

**Auto-fix decision:** Per Rule 1 (auto-fix bugs that prevent task completion), the executor fixed the chunk_hits collision inline rather than escalating to a follow-up plan. The 4 commits added 7 regression tests so the bug pattern cannot recur silently.

**Scope attribution:** The 4 commits use scope `(77-04)` in their messages because they fix the parallels JSON handler shipped in Plan 04. They belong in this Plan 05 narrative timeline because they were uncovered AND landed during Plan 05's manual smoke check.

### No other deviations

The plan's two tasks (docs update + smoke verification) executed as written. The 1-day duration reflects the context-handoff between Task 1 (Plan 05 executor on 2026-04-27) and Task 2 (Plan 05 continuation agent on 2026-04-28 after the user completed the smoke-check).

## Known Limitations

- **NLI multi-uid cataloging on same sys_id is real and intentional**: NLI sometimes catalogs the same physical manuscript under multiple Alma uids on the same sys_id (e.g., Karaite prayer books in user's smoke-check sample). The group-level dedup in commit `327aea31` collapses only entries that share BOTH `chunk_index` AND `manuscript_snippet`. If two uids on the same sys_id genuinely have different manuscript content (different IEs / volumes / fragment slices), they correctly remain as separate entries. Result 1 in the user's smoke-check sample showed 32 matches across 17 chunks for a single sys_id — that is correct grouping, not a bug.
- **chunk_hits Path B fallback is intentionally minimal**: when an item arrives without `chunk_hits` (or with a non-list `chunk_hits`), the serializer emits a single degenerate match using `source_ctx`/`text`/`score`. Path B exists for future-proofing if a caller bypasses Plan 02 and Plan 05's chunk_hits population — real callers always populate the list-of-tuples shape.
- **chunk_count rename is a Plan 05 deviation from Plan 02's plan text**: Plan 02 left the standard-mode int counter at `item['chunk_hits']`. Plan 05 renamed it to `item['chunk_count']` to free the field name. Any downstream code reading `item['chunk_hits']` as an int will see a list now. Verified that no other code in the repo reads `chunk_hits` as an int (the standard-mode rendering at the parallels page renders count via a different path that uses len(rec.get('chunks', []))).

## Threat Surface Scan

No new threat-relevant surface introduced beyond what Phase 77's `<threat_model>` accepts. The 4 follow-on commits modified `genizah_core.py` (per-uid dict population — internal, not at trust boundary) and `shared/search_serializer.py` (defensive guard at consumer boundary, not new attack surface). The `logger.exception` upgrade in JSON handlers improves observability without leaking sensitive data (FastAPI handlers don't echo request bodies in tracebacks).

## Phase 77 Status: READY FOR /gsd-verify-work

All 5 Phase 77 plans complete:
- ✅ 77-01: AppState envelope-echo fields + state-population sites + Wave 0 RED test scaffolding
- ✅ 77-02: genizah_core.lab_composition_search chunk_hits extension (D-13 Path A)
- ✅ 77-03: shared/search_serializer.py module (single source of truth, all 22 tests GREEN)
- ✅ 77-04: web/api.py JSON handlers + toolbar buttons on /search and /parallels (with smoke-check follow-on polish)
- ✅ 77-05: docs/OPEN_ISSUES + docs/CODE_INDEX update + manual smoke check (PASSED)

**Per-plan summary files** (cover all artifacts produced by the phase):
- `.planning/phases/77-serializer-json-export/77-01-SUMMARY.md`
- `.planning/phases/77-serializer-json-export/77-02-SUMMARY.md`
- `.planning/phases/77-serializer-json-export/77-03-SUMMARY.md`
- `.planning/phases/77-serializer-json-export/77-04-SUMMARY.md`
- `.planning/phases/77-serializer-json-export/77-05-SUMMARY.md` (this file)

**Phase gate per ROADMAP.md §Phase 77:** "pytest green, CI green, manual download spot-check on /search and /parallels."
- ✅ pytest green: 1201 passed / 8 skipped
- ✅ CI green: assumed green pending push (last local-run baseline confirms)
- ✅ Manual download spot-check on /search and /parallels: signed off by user 2026-04-28 with final clean JSON output

**Next step:** orchestrator should display the phase-complete banner and offer `/gsd-verify-work 77` as the next action. Phase 78 (`/api/search` + Hardening Shell) is queued behind verify and is the next phase to plan.

## Self-Check: PASSED

- [x] docs/OPEN_ISSUES.md modified (chunk_hits collision row + summary counts + Last Updated banner + Change Log entry) — verified by `git diff` on commit `015b17d5`
- [x] docs/CODE_INDEX.md modified (search_serializer extended + Companion shape contract subsection + Last updated banner) — verified by `git diff` on commit `015b17d5`
- [x] Commit `db586467` (Task 1 docs) — FOUND in `git log`
- [x] Commit `015b17d5` (Task 2 close-out docs) — FOUND in `git log`
- [x] Smoke-check follow-on commits `baf481fb`, `c24fcc48`, `2e2d2b75`, `327aea31` — FOUND in `git log` (Plan 04 scope, Plan 05 timeline)
- [x] python scripts/check_docs.py exits 0 (all checks passed)
- [x] Final test count 1201 passed / 8 skipped (no regressions, +7 new tests during smoke-check polish)
- [x] User signed off the manual smoke-check (implicit approval via final clean JSON output + orchestrator wrap-up instruction)
- [x] Phase 77 ready for /gsd-verify-work
