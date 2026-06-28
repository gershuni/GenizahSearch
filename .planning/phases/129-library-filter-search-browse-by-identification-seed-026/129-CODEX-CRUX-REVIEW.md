# Phase 129 — Codex Design-Crux Review (Success Criterion #4 gate)

**Date:** 2026-06-28
**Reviewer:** Codex (codex-cli 0.139.0), `codex exec` against live source
**Target:** the catalog push-down design crux resolved in `129-RESEARCH.md` § Design Crux Resolution
**Verdict:** **APPROVE WITH CHANGES**

> This satisfies the SEED-026 / ROADMAP Success-Criterion-#4 "Codex-review-before-code gate": the design crux (catalog row → `library_code` mapping + cheapest push-down query shape) is reviewed and approved-with-changes **before** any implementation. The two required changes below are MANDATORY inputs to the planner.

---

## Design APPROVED (verified against live source)

1. **Push-down placement correct.** PGP/Editions append to `conditions` (`shared/fjms_service.py:2206`, `:2216`); `where` is built once at `:2226` and feeds BOTH the COUNT (`:2228`) and the results query before `LIMIT/OFFSET` (`:2248`). A library `EXISTS` clause added there (before `where = ...`) gets correct `total` + pagination. No post-limit risk.
2. **Reverse lookup sound.** `csv_bank` keys = normalized `sys_id` (col 0, `metadata_manager.py:303`); `library_code` = col 3 (`:324`); `get_library_for_id` uses the same shape (`:413`). The `{sid for sid,row in csv_bank.items() if row.get('library_code') in selected}` comprehension is correct.
3. **`AlmaId == sys_id` confirmed.** FJMS catalog `AlmaId` is `TEXT`, exported from `alma.AlmaId` (`scripts/export_fist_enrichment.py:303`, `:346`). The TEMP table keyed by `AlmaId` correctly matches sys_ids from `csv_bank` for current numeric Alma/sys IDs.
4. **Allowlist extension safe.** Extending `_FILTER_TEMP_TABLES` (`:1990`) is sufficient and injection-safe (names never interpolated from caller input).
5. **Composition works.** All three `EXISTS` clauses AND together via `" AND ".join(conditions)` (`:2226`). TEMP lifecycle is per-thread/per-connection (`thread_local_db.py:83`, `:114`).
6. **Performance acceptable.** O(255K) `csv_bank` comprehension off the event loop + a ~128K-row TEMP table (`AlmaId TEXT PRIMARY KEY`, `:2013`) is fine. No DB migration required.

---

## REQUIRED CHANGE 1 — HIGH: dynamic temp-table token (correctness bug)

**Problem:** RESEARCH.md's worked example passes `len(library_sys_ids)` as the `_ensure_filter_temp` `token`. `_ensure_filter_temp` skips rebuilding the TEMP table when `reg.get(name) == token` (`shared/fjms_service.py:2009`). That is safe for PGP/Editions because their sys_id sets are **static, corpus-wide** (a given filter value always maps to the same set — the docstring at `:1992-1997` says token is "currently `len`" for exactly this reason). The library filter is **dynamic multi-select**: two *different* selections that resolve to the same set **size** would collide on the `len` token and **reuse stale TEMP rows** → wrong results within one thread/connection across page turns or filter changes.

**Fix (planner MUST encode):** derive the token from the **selection content**, not its length — e.g. `hash(tuple(sorted(library_codes)))` (or a deterministic digest; keep it an `int` to match the param type). Update the `_ensure_filter_temp` docstring/comment (`:1988-1997`) since the library set is no longer "static, corpus-wide". **Test:** on ONE service/thread, switch between two **same-size but different** selections and assert `total`/results change (the test that would have caught this).

## REQUIRED CHANGE 2 — MEDIUM: "selected but resolves to empty" handling

**Problem:** A non-empty `library_codes` that resolves to an empty `library_sys_ids` set silently fails open (`_ensure_filter_temp` returns `False` for empty `sys_ids` at `:2003`). Correct for the *empty selection = all* case, but it masks (a) invalid/garbage persisted values and (b) `csv_bank` not yet loaded — both would silently show ALL results instead of the intended filter.

**Fix (planner MUST encode):**
- Normalize persisted state to `list[str]` defensively (RESEARCH Pitfall 6 already flags this — `safe_user_get('...', [])` then `isinstance(...) list` guard).
- Validate selected codes against `LIBRARY_CODES` keys; drop unknown codes.
- Establish a clear **`csv_bank` readiness** policy before catalog library-filtering is enabled (don't resolve against a half-loaded bank).
- Distinguish + **log/test** the "selected non-empty but resolved empty" path (e.g. all selected codes invalid, or bank unready) vs. the legitimate "empty selection = all" path.
- Keep the invariant: empty selection passes `library_codes=[]/None` AND `library_sys_ids=None` (never `set()`).

---

## Planner checklist (Codex item 7)

The planner MUST encode tasks/tests for: dynamic content-derived token; active-filter test that `total` is correct **before** pagination; empty-selection no-op test; allowlist-membership test + a behavior test proving the filter changes `total`; PGP+Editions+Library 3-way composition test; persisted-list normalization/validation; and a `csv_bank` readiness policy. (These map onto the RESEARCH § Validation Architecture Wave-0 files.)

**VERDICT: APPROVE WITH CHANGES** — proceed to planning with REQUIRED CHANGE 1 (token) and REQUIRED CHANGE 2 (selected-but-empty + readiness) folded in.
