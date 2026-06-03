---
phase: 106
reviewers: [codex]
reviewed_at: 2026-06-03T14:00:24Z
plans_reviewed: [106-01-PLAN.md, 106-02-PLAN.md, 106-03-PLAN.md]
review_focus: plan↔code drift (Codex has live repo read access)
verdict: NEEDS REVISION
---

# Cross-AI Plan Review — Phase 106 (Joins Lab Shared Core)

Pre-flight cross-AI review run after the internal gsd-plan-checker passed, per standing workflow
preference: the internal checker validates plan-*internal* consistency; Codex (with live repo
read access) cross-checks every signature/line/behavior claim against the actual `genizah_core.py`
and `shared/*.py`. Model: codex-cli 0.136.0, sandbox bypassed (read-only review of own repo;
Windows sandbox spawn failed on first attempt).

## Codex Review

**Summary**
The plans are close, but should not execute as-is. The critical page-anchor R-01 assumption is
correct, but R-02's bracket-safety claim is only partially true in the live line-break path, and
Plan 01's planned imports will fail the repo's Ruff `F401` check. There is also a real signature
drift on `execute_search(..., corpus_scope=...)`.

### Plan↔Code Drift Findings

1. **VERIFIED — `_parse_line_break_query`.** `class LineGroup` (`.components/.line_start/.line_end`)
   at genizah_core.py:5799-5800; `_parse_line_break_query` at :5811 returning `(groups, line_gaps)`
   at :5927. Direct import succeeds without engine construction. (R-01 import-safety confirmed.)

2. **MISMATCH, HIGH — bracket helpers exist, but R-02 is overstated for line-break searches.**
   Helpers real: `_query_has_brackets`:6342, `_strip_brackets`:6352. The line-break path strips
   brackets for regex matching at :8124-8125 and position filtering at :8142-8150. BUT line-break
   Tantivy expansion uses `_expand_responsa_component()`:8019 (body :7885-7920), which does **not**
   add `_add_bracket_variants()` — normal search does at :7543-7549. Also page-scope line-break
   results are only appended if `highlight(content, regex)` matches original content (:8193-8200),
   and `highlight()` returns `None` on no raw regex match (:7683-7685).
   **Fix:** revise R-02 / Plan 03 claims and tests (the Plan 03 self-match test only proves the pure
   `detect_self_match()` checks `sys_id` membership in supplied results — it does NOT prove the engine
   returns bracket-prefixed line-start hits), or add bracket variants + bracket-tolerant highlighting
   to the line-break engine path before relying on bracket-safe self-match.

3. **MISMATCH, MEDIUM — `execute_search` signature default.** Actual (:8298):
   `execute_search(..., restrict_sys_ids=None, text_position=None, corpus_scope="all")` — NOT
   `corpus_scope="genizah"`. R-01 itself VERIFIED: `text_position` forwarded into
   `_execute_line_break_search` at :8356-8365, applied at :8142-8153.
   **Fix:** quote the live default `"all"` in the Protocol, document `"genizah"` as a Joins Lab adapter
   policy, and keep passing `corpus_scope="genizah"` explicitly.

4. **VERIFIED — `get_browse_page`.** Signature (:9483):
   `get_browse_page(self, sys_id, p_num=None, next_prev=0, absolute_index=None, allow_cross=False, volume_ie=None)`.
   Returns `uid, p_num, full_header, text, total_pages, current_idx, internal_index, sys_id, volume_ie`
   (:9568-9578); metadata fallback also includes `text`/`total_pages` (:9471-9480).

5. **MISMATCH, LOW — result dict shape mostly matches, but `score` is not guaranteed.** `display`
   shape verified via `get_display_data()` :5249-5256. Regular results include `score` at :8681/:8694,
   but **line-break results (:8180-8200) include `display/snippet/full_text/uid/highlight_pattern/scope`
   with NO `score`.** **Fix:** keep `Candidate.score: Optional[float] = None` and add a missing-score
   line-break fixture.

6. **VERIFIED — shared services.** `get_vs_service()` at visual_similarity_service.py:312;
   `get_suggestions(sys_id, limit=200)` :97 returns `alma_id/svm_score/rank` :122-125.
   `get_fjms_service()` at fjms_service.py:3417; `get_measurements()` :2925.

7. **VERIFIED — Plan 01 imports module `dataclasses`.** 106-01-PLAN.md:128; Plan 02 uses
   `dataclasses.replace(...)` at 106-02-PLAN.md:220 and :231. (The prior internal-checker blocker fix
   confirmed landed.)

8. **VERIFIED — pattern analogs.** `shared/refinement.py` imports both dataclass forms :19-20;
   `shared/fist_cudl_bridge.py` frozen-dataclass convention :80-97. `typing.Protocol`/`runtime_checkable`
   genuinely new to production code (only comment/docstring mentions found).

### Strengths
Good phase boundary (pure shared module, thin `SearchExecutor`, no UI, no direct sidecar DB). R-01
resolution is correct and avoids an unnecessary two-query intersection API. Practical TDD breakdown.
`Candidate` normalizer as single source of truth is the right direction.

### Other Concerns
- **HIGH — Plan 01 will likely fail Ruff.** `ruff.toml` selects `F401` (:15-19). The planned module
  imports `field`, `_parse_line_break_query`, `_query_has_brackets`, `_strip_brackets` but the planned
  implementation does not use them. Plan 01 Task 3's own acceptance criterion runs `ruff check` → the
  plan contradicts itself.
- **MEDIUM — SC#3 key wording.** ROADMAP SC#3 (:123) says the canonical key includes "side image /
  adjacent-side membership"; Plan 01 uses only `(sys_id, page)`. May be intentional (per-image dedup
  with side/membership as merged provenance, not key components) but must be explicitly documented.
- **LOW — wording drift.** Plan 01 top-level must-have says page-position placement is "rejected at
  model construction"; the task body enforces it in `compose()`. Align the wording.
- **LOW — VS handling.** Mostly right: `vs_score=None` documented as "no VS data." Keep tests from
  treating missing VS as negative evidence.

### Suggestions
Remove unused imports from `shared/joins_lab.py`; let tests import `_parse_line_break_query` directly
from `genizah_core`. Import bracket helpers only if the module uses them, or re-export with `__all__`.
Revise R-02 tests or downgrade the claim. Change the Protocol signature to live
`execute_search(..., corpus_scope="all")`, then pass `"genizah"` explicitly in Joins Lab calls.

### Risk Assessment
Overall as-written: **HIGH** for execution readiness — bracket behavior claim is unsafe for line-break
self-match and the planned file likely fails its own Ruff gate. **Verdict: NEEDS REVISION.**
**Must-fix before execution:** (1) R-02 claim/test, (2) unused imports vs Ruff `F401`, (3) `corpus_scope`
signature drift.

---

## Consensus Summary

Single reviewer (Codex), chosen specifically for live-repo signature verification — the dimension the
internal checker cannot cover.

### Agreed Strengths
- Correct R-01 resolution (one `execute_search` call via `text_position`, no two-query intersection).
- Clean architecture: pure shared module + thin `SearchExecutor` Protocol + single `Candidate` normalizer.

### Must-Fix Before Execution (highest priority)
1. **Ruff F401 (HIGH):** remove unused module imports (`field`, `_parse_line_break_query`,
   `_query_has_brackets`, `_strip_brackets`) — the plan would fail its own `ruff check` acceptance gate.
   Tests import `_parse_line_break_query` directly from `genizah_core`.
2. **R-02 bracket claim (HIGH):** correct the RESEARCH.md R-02 statement and Plan 03 framing — the
   line-break path does not expand bracket variants, so "engine auto-strips tear-brackets" is not a
   safe end-to-end self-match guarantee. `detect_self_match()` is a pure sys_id-membership check; scope
   the claim/test to what it actually proves (do NOT expand into engine changes — out of phase scope).
3. **`corpus_scope` default (MEDIUM):** document the Protocol default as the live `"all"`; Joins Lab
   passes `"genizah"` explicitly.

### Should-Fix (clarifications, no execution risk)
- Document why SC#3 canonical key is `(sys_id, page)` (per-image dedup; side/adjacent-side as merged
  provenance, not key) vs ROADMAP's 4-tuple wording.
- `Candidate.score` stays `Optional[float] = None`; add a missing-`score` line-break fixture.
- Align "rejected at model construction" wording with `compose()`-time enforcement.

### Divergent Views
None — single reviewer.
