---
phase: 119
review_type: gap-closure-preflight
round: 2
reviewers: [codex]
reviewed_at: 2026-06-19
plans_reviewed: [119-09-PLAN.md, 119-10-PLAN.md, 119-11-PLAN.md]
verdict: REVISE
resolution: all 4 findings folded into plans (see below)
brief: $CLAUDE_JOB_DIR/tmp/codex-119-r2-preflight-brief.md
raw_output: $CLAUDE_JOB_DIR/tmp/codex-119-r2-preflight-output.md
---

# Phase 119 ROUND-2 GAP-CLOSURE — Cross-AI Plan Pre-flight (Codex)

**Reviewer:** Codex CLI (codex-cli 0.139.0), read-only sandbox, against live source.
**Subject:** the 3 NEW round-2 gap-closure plans (119-09/10/11) closing live-UAT gaps R2-1..R2-10.
**Verdict:** **REVISE** — 2 HIGH, 2 MEDIUM. Value: plan↔code DRIFT the internal plan-checker (plan-internal consistency, 0 blockers) cannot see — same gate that caught 1 BLOCKER + 4 HIGH on the round-1 gap plans (`119-REVIEWS-gaps.md`).

> Codex ran read-only; static, code-grounded review.

## Findings (all resolved)

### P119-R2-8-1 [HIGH] — `get_browse_page` returns a dict, not `.text` (119-10, R2-8)
`web/joins_executor.py:87-89`, `shared/joins_lab.py:193-194/466-468`. Plan said fetch transcription via `page.text`; the live contract returns a dict and existing code reads `page_data.get("text", "")`.
**Resolution:** 119-10 action step 1 + read_first + acceptance updated to use `page_data = executor.get_browse_page(...); text = page_data.get("text", "") if page_data else ""` (NOT `.text`).

### P119-R2-8-2 [MEDIUM] — new lookup evades the off-loop guard (119-10, R2-8)
`tests/test_joins_lab_off_loop.py:520-523`. The VS-meta off-loop guard only scans `get_meta_for_id` / `get_library_for_id`; the new `get_browse_page` call would evade it.
**Resolution:** 119-10 adds `tests/test_joins_lab_off_loop.py` to `files_modified` + action step 1b + acceptance: add `get_browse_page` to the guard's blocking-method list (verified the list is at :520-522).

### P119-R2-7-1 [HIGH] — stale hidden-dialog keyboard handler stays globally live (119-11, R2-7)
`nicegui/elements/dialog.py:21-23`, `web/pages/joins_lab.py:641-649`, `web/components/compare_modal.py:254`. NiceGUI dialogs are HIDDEN, not removed; `_open_compare` creates fresh dialogs without deletion, so an Esc `ui.keyboard` from a stale Compare keeps firing globally.
**Resolution:** 119-11 R2-7 action step 4 + acceptance updated: guard the handler with `if not dialog.value: return` (or bind `active` to dialog state / delete on close) + a unit-test no-op-when-closed assertion.

### P119-R2-2-1 [MEDIUM] — fixed chevron icons don't flip under RTL (119-11, R2-2)
`web/components/compare_modal.py:523-551`. Buttons carry fixed `icon="chevron_left/right"` that won't translate or flip; fixing only the text labels leaves the chevrons wrong.
**Resolution:** 119-11 R2-2 action step 2 + acceptance updated: remove the `icon=` arg (rely on labelled chevron) OR swap icon by `get_language()`; assert no hardcoded LTR chevron icon contradicts the RTL label.

## Confirmations (checked out against live code)
- R2-1 tr() literals exact: `tr("Anchor")`, `tr("Candidate")`, `tr("‹ Prev")`, `tr("Maybe")`, `tr("Next ›")` — all absent from TRANSLATIONS; `Yes`/`No`/`Compare` present.
- `shared/joins_lab.py` import-safe for `TRIAGE_ICONS` — no `web/*` imports, no circular dependency.
- Triage render sites: grid `candidate_grid.py:697-738`, compare `compare_modal.py:532-545`.
- `Candidate` is frozen; VS-meta path uses `dataclasses.replace` inside `run.io_bound(run_vs_meta_core)` and does not currently fetch `full_text`.
- Dark-mode mechanism + missing candidate-table rule match; `.joins-candidate-table` feasible (mirrors `.catalog-browse-table .q-table`).
- AnchorViewer has the duplicate shelfmark header, guarded label updates, current `max-height: 72vh`; defaulted args preserve non-Compare callers. Call sites: `joins_lab.py:1265`, `compare_modal.py:375/492`, tests `test_anchor_viewer.py:131/519/561/679/780/848`.
- `ui.keyboard` API real; render-smoke can synthesize the keydown via the keyboard element's `key` listener args.
- 119-10/119-11 serialization removes the shared test-file collision; no other shared `files_modified`.
- No server-side `stop_propagation()` or raw-storage bypass in target files.
