---
phase: 89-lists-cache-per-request
mode: reviews-revision
revised_at: 2026-05-15
revised_by: Claude Opus 4.7 (1M context)
source_reviews: 89-REVIEWS.md (Gemini + Codex)
plans_touched: [89-01-PLAN.md, 89-02-PLAN.md]
---

# Phase 89 — Revisions Log

Audit log of the 10 targeted revisions applied to Plans 89-01 and 89-02 in
response to the Gemini + Codex cross-AI review (`89-REVIEWS.md`, 2026-05-15).

The revisions sharpen instructions; they do NOT renegotiate any CONTEXT.md
locked decisions (D-01..D-11). Plan structure, frontmatter, threat model,
and verification sections are preserved.

## Revisions Applied to Plan 89-01

| ID  | Severity / Reviewer | Plan / Task touched                      | Edit applied (1-sentence summary)                                                                                                                          |
|-----|---------------------|-----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| R1  | Codex HIGH          | 89-01 Task 3 (behavior tests)            | Added 4th test `test_captured_manager_does_not_serve_stale_data_after_user_switch` that uses ONE captured `UserListsManager` across a user-id patch switch — directly proves the D-03 captured-manager case (the original 3-test set only proved factory behavior). |
| R2  | Codex MEDIUM        | 89-01 Task 1 Edit 2 (`_get_cached_data`) | Inserted defensive `if not user_id: return self._get_default_data()` between the `is_authenticated` check and the Supabase fetch — prevents `get_user_lists(None)` in the edge case where `is_logged_in()==True` but `get_user_id()==None`.                       |
| R3  | Codex LOW (+ Gemini implicit) | 89-01 Task 1 (new Edit 3.5)    | Added conditional import-cleanup step: grep `time` and `Tuple` AFTER Edits 1–3, remove each from imports IFF no remaining reference in the file. Acceptance criteria record the decision in SUMMARY.md.                                                          |
| R4  | Gemini LOW          | 89-01 Task 1 Edit 2 + `<read_first>`     | Added explicit pre-rewrite check: re-read current `_get_cached_data` body for any `try/except` blocks; if present, the stateless rewrite MUST preserve equivalent semantics. Documented as a separate "R4 preservation step" in Task 1 action.                    |
| R5  | Gemini LOW          | 89-01 Task 1 (new Edit 4.5)              | Added one-time grep for `class.*ListsManager` and `def create_project` subclass overrides across `genizah_app.py`, `shared/`, `web/`. Expected zero hits; if any subclass override exists, STOP and flag before finalizing the `color=None` change.              |
| R6  | Codex Suggestion    | 89-01 Task 4 (delegation audit)          | Replaced "default to deferring" language with explicit trichotomous classification per call site (CLEAN / COSMETIC DRIFT / RUNTIME TYPEERROR). RUNTIME-TYPEERROR findings MUST be flagged to orchestrator BEFORE plan boundary, NOT silently deferred.            |

## Revisions Applied to Plan 89-02

| ID  | Severity / Reviewer        | Plan / Task touched                        | Edit applied (1-sentence summary)                                                                                                                                                                                                                                              |
|-----|----------------------------|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| R7  | Codex HIGH + Gemini MEDIUM | 89-02 Task 1 (AST scanner scope)           | Narrowed `_cache_entry`/`_cache_ttl` unrestricted-attribute catch to `NARROW_SCOPE_OWNING_FILES = {web/user_lists.py, web/state.py}`. Outside those files, only state-alias-qualified or self-qualified accesses are flagged. Added `test_scanner_narrow_scope_for_cache_entry_outside_owning_files` + `test_exempt_files_are_skipped_in_production_scan`. |
| R8  | Codex MEDIUM               | 89-02 Task 1 (AST scanner Call-node)       | Added `visit_Call` matcher for `<state-alias>.init_user_lists_mgr(...)` shape (catches restored caller without method body, and converse). New seed-trap test `test_scanner_catches_init_user_lists_mgr_call_site` validates with `state.init_user_lists_mgr()` + aliased `s.init_user_lists_mgr()` forms.                                       |
| R9  | Gemini MEDIUM              | 89-02 Tasks 1, 2, 3 (CI green sequencing)  | Added `@pytest.mark.xfail(strict=True, reason="...")` markers on `test_no_deleted_lists_references_in_web_and_tests` (Task 1) and `test_appstate_does_not_have_user_lists_mgr` (Task 2). Task 3 atomic commit now includes Edit 4 + Edit 5 that REMOVE both markers in the same commit as the field deletion — CI stays green across the entire plan execution. |
| R10 | Codex HIGH                 | 89-02 Task 3 (atomic commit ops gates)     | Added explicit pre-commit `git diff --cached --name-only \| sort` and post-commit `git show --stat --name-only HEAD` operational gates. Atomic-commit file boundary expanded to **5 files** (reconciling with R9): `tests/test_no_appstate_export_fields.py`, `tests/test_no_deleted_lists_state_references.py`, `tests/test_no_user_lists_mgr_field.py`, `web/main.py`, `web/state.py` (alphabetically sorted). Any divergence halts the commit. |

## Cross-Cutting Reconciliation

- **R9 ↔ R10 interaction:** R9 expanded the atomic-commit file set from 3 → 5
  (added the two test files that hold the xfail markers). R10's pre/post-commit
  gates were rewritten to verify the exact 5-file sorted set, not the original
  3-file set. Both reviewers' acceptance criteria simultaneously satisfied.

- **D-09 atomicity discipline:** Strengthened (not relaxed). The atomic-commit
  boundary now has explicit operational verification — Codex's
  "operational enforcement of D-09" concern is closed.

- **CONTEXT.md decisions D-01..D-11:** Preserved verbatim. No revision touches
  the locked decisions. The revisions sharpen implementation specificity within
  the decision boundaries.

- **Threat model:** Plan 89-01 added T-89-09 (R2 edge-case mitigation). Plan
  89-02 added T-89-10 (R7 scanner false-positive mitigation). Existing threats
  T-89-01..T-89-08 unchanged.

- **Frontmatter:** Plan 89-02 `files_modified` unchanged at the file-list level
  (the two test files were always going to be modified in Task 3 to remove
  xfail markers; they're listed). Plan 89-01 `files_modified` unchanged.
  `must_haves.truths` updated in both plans to reflect new acceptance criteria.

## Verification That Quality Gate Is Closed

- [x] 89-01-PLAN.md updated in place addressing R1..R6
- [x] 89-02-PLAN.md updated in place addressing R7..R10
- [x] Frontmatter preserved (phase, plan, type, wave, depends_on, files_modified, autonomous, requirements, must_haves)
- [x] CONTEXT.md decisions D-01..D-11 preserved
- [x] D-09 atomic-commit boundary now has explicit pre/post-commit operational gates (R10)
- [x] xfail markers added in Task 1/2 AND removed in Task 3 (R9) — atomic-commit file count reconciled with R10 (5 files)
- [x] All LISTS-01..04 requirements still covered (LISTS-02, LISTS-03, LISTS-04 in Plan 89-01; LISTS-01, LISTS-03 in Plan 89-02)
- [x] REVISIONS-LOG.md (this file) written listing R1..R10 applied edits
