---
phase: 106-joins-lab-shared-core-pure-logic-service-adapters-unit-tests
fixed_at: 2026-06-03T00:00:00Z
review_path: .planning/phases/106-joins-lab-shared-core-pure-logic-service-adapters-unit-tests/106-REVIEW.md
iteration: 1
findings_in_scope: 2
fixed: 2
skipped: 3
status: all_fixed
---

# Phase 106: Code Review Fix Report

**Fixed at:** 2026-06-03
**Source review:** .planning/phases/106-joins-lab-shared-core-pure-logic-service-adapters-unit-tests/106-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope (critical_warning): 2
- Fixed: 2
- Skipped: 3 (all 3 Info findings — out of scope for critical_warning)

Both in-scope Warnings were fixed, each with a dedicated regression test. After
both fixes: `python -m ruff check shared/joins_lab.py tests/test_joins_lab.py`
passes clean, and `python -m pytest tests/test_joins_lab.py -q` reports
**69 passed** (66 original + 3 new regression tests).

## Fixed Issues

### WR-02: merge_candidates() drops vs_score when annotating an overlapping text candidate

**Files modified:** `shared/joins_lab.py`, `tests/test_joins_lab.py`
**Commit:** 4f1c306e
**Applied fix:** In `merge_candidates()`, the overlap-annotation `dataclasses.replace`
call now also carries `vs_score=v.vs_score` (previously only `via_vs` + `vs_rank`
were copied). This stops a candidate that genuinely has VS data from being
re-stamped with `vs_score=None`, which the `Candidate` docstring defines as "no VS
data (NOT 0.0 dissimilar) — Pitfall 6". Added regression test
`TestMerge::test_overlap_carries_vs_score` asserting the merged overlap candidate
reports `via_vs=True`, `vs_rank=4`, and `vs_score=0.91` (the VS value is preserved).

### WR-01: htmlify() sentinel pre-injection — raw SOH/STX bytes silently treated as a highlight region

**Files modified:** `shared/joins_lab.py`, `tests/test_joins_lab.py`
**Commit:** 8293c568
**Applied fix:** `htmlify()` now strips any raw `MARK_A` (`\x01`) / `MARK_B` (`\x02`)
sentinel bytes from the input (`text = (text or "").replace(MARK_A, "").replace(MARK_B, "")`)
before the regex match-substitution step, so untrusted corpus content can no longer
forge a highlight region. Engine transcription text is externally sourced, so the
prior "control bytes never appear in corpus text" assumption was enforced rather than
assumed. The docstring processing-order note gained a step 0 describing the strip.
Added two regression tests in `TestSnippet`:
`test_htmlify_strips_injected_sentinels` (injected `\x01injected\x02` no longer gets
a stray `<b>` wrapper; the legitimate pattern match still highlights exactly once;
no raw sentinel bytes survive) and `test_htmlify_strips_sentinels_without_pattern`
(stripping applies even when no pattern is supplied). Also added `htmlify` to the
test module imports.

## Skipped Issues

### IN-01: `_match_line` assumes every element of `lines` is a string

**File:** `shared/joins_lab.py:600-602`
**Reason:** Out of scope — Info finding (fix_scope is critical_warning). The reviewer
also notes this is not currently reachable (all callers pass `(text or "").split("\n")`,
which only yields strings) and explicitly classifies it "low priority — purely a
hardening note."
**Original issue:** `rx.search(ln)` would raise `TypeError` if a non-string element
sneaked into `lines`; it is an undocumented precondition on a module-level helper.

### IN-02: `runtime_checkable` Protocol membership check is name-only

**File:** `shared/joins_lab.py:149-150`, `tests/test_joins_lab.py:161-163`
**Reason:** Out of scope — Info finding (fix_scope is critical_warning). The reviewer
states "No change required" and notes this is a known/documented limitation of
`runtime_checkable` Protocols, not a defect in this code.
**Original issue:** `@runtime_checkable` makes `isinstance(x, SearchExecutor)` check
only that the four method names exist (no signature/arity validation), so
`test_fake_is_searchexecutor` asserts less than it appears to.

### IN-03: `apply_cross_side` OR-path note double-counts pages already present in base

**File:** `shared/joins_lab.py:415-462`
**Reason:** Out of scope — Info finding (fix_scope is critical_warning). The reviewer
states "There is no bug" and classifies the inconsistent note phrasing as "Cosmetic"
with an "Optional" fix.
**Original issue:** The AND-path note (`f"B matched {len(b_set)} pages"`) and the
OR-path note (`f"B matched {len(b_set)} pages · +{added} via other side"`) use
different phrasings for the same `b_set` count; a downstream UI displaying them
side-by-side may want consistent wording.

---

_Fixed: 2026-06-03_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
