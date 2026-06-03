---
phase: 106-joins-lab-shared-core-pure-logic-service-adapters-unit-tests
reviewed: 2026-06-03T14:57:07Z
depth: standard
files_reviewed: 2
files_reviewed_list:
  - shared/joins_lab.py
  - tests/test_joins_lab.py
findings:
  critical: 0
  warning: 2
  info: 3
  total: 5
status: issues_found
---

# Phase 106: Code Review Report

**Reviewed:** 2026-06-03T14:57:07Z
**Depth:** standard
**Files Reviewed:** 2
**Status:** issues_found

## Summary

`shared/joins_lab.py` is a well-structured, defensively-written pure-logic module. All the
architectural constraints called out in the review brief are honored and verified:

- **No Qt / no DB:** No PyQt/PySide imports, no `sqlite3.connect`, no `fist_data` path. Static
  AST guards in the test file enforce this and pass.
- **F401-clean:** `python -m ruff check` passes with no findings on both files. Every import
  (`dataclasses`, `html`, `re`, `dataclass`, `Optional`, `Protocol`, `runtime_checkable`) is used.
- **Frozen dataclasses:** All four dataclasses are `frozen=True`; mutation goes through
  `dataclasses.replace()` in `dedup_candidates` and `merge_candidates`.
- **Regex safety:** `try/except re.error` guards both `_match_line` and `htmlify`; malformed
  patterns degrade to no-match / no-highlight rather than raising.
- **None/missing-key robustness:** `normalize_candidate({})` produces a valid Candidate with safe
  defaults (no KeyError); verified against fully-empty and partial dicts.
- **compose() page-anchor guards:** ValueError placement guards run *before* the empty
  short-circuit (verified) and round-trip correctly through the real engine parser
  `_parse_line_break_query` for single-row, multi-row, line_start, line_end, and `[|N]` gap cases.
- **html.escape ordering:** Corpus markup (`<script>`, `<b>`, `&`) is fully neutralized — escape
  runs after sentinel substitution, and only this module's own `<b style=...>` tag survives.

All 66 tests pass. The findings below are residual robustness/fidelity gaps, none of which block
the phase. The two Warnings are worth addressing before the I/O adapters (Phase 107+) start
feeding real engine output through this module.

## Warnings

### WR-01: htmlify() sentinel pre-injection — corpus text containing raw SOH/STX bytes is silently treated as a highlight region

**File:** `shared/joins_lab.py:608-638`
**Issue:** `MARK_A` (`\x01`) and `MARK_B` (`\x02`) are used as pre-escape highlight sentinels.
The docstring asserts "These characters (SOH/STX) are not valid in corpus text" — but this is an
unverified assumption, not an enforced invariant. If a result dict's `full_text` already contains
raw `\x01`/`\x02` bytes, they pass through `html.escape()` unchanged (escape does not touch control
chars) and get rewritten into `<b style=...>`/`</b>` in step 4. Verified:

```python
htmlify('normal \x01injected\x02 text', 'normal')
# -> "...<b ...>normal</b> <b ...>injected</b> text..."   # 'injected' got a stray highlight
```

This is **not** a markup-injection XSS — the attacker cannot emit arbitrary tags, only the fixed
`<b style='color:#dc2626'>` wrapper — so it is a Warning, not Critical. The impact is corrupted /
misleading highlight rendering driven by untrusted corpus content. Engine transcription text is
externally sourced, so "control bytes never appear" should be enforced, not assumed.

**Fix:** Strip the sentinel bytes from the input before substitution so corpus content can never
forge a highlight region:

```python
def htmlify(text: str, pattern: Optional[str] = None) -> str:
    text = (text or "").replace(MARK_A, "").replace(MARK_B, "")
    if pattern:
        try:
            rx = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
            text = rx.sub(lambda m: MARK_A + m.group(0) + MARK_B, text)
        except re.error:
            pass
    ...
```

### WR-02: merge_candidates() drops vs_score when annotating an overlapping text candidate

**File:** `shared/joins_lab.py:535-539`
**Issue:** When a text candidate and a VS candidate share a `sys_id`, the text candidate is
re-stamped via `dataclasses.replace(r, via_vs=True, vs_rank=v.vs_rank)` — but `v.vs_score` is not
copied. The merged candidate then reports `via_vs=True` with `vs_score=None`, which the
Candidate docstring explicitly defines as "no VS data (NOT 0.0 dissimilar) — Pitfall 6". So a
candidate that genuinely *does* have VS data is mislabeled as having none. Verified:

```python
merge_candidates(
    [Candidate(sys_id='X', page=5, via_text=True)],
    [Candidate(sys_id='X', page=None, via_vs=True, vs_rank=4, vs_score=0.91)],
)
# merged X: via_vs=True, vs_rank=4, vs_score=None   # 0.91 lost
```

This contradicts the very Pitfall-6 semantics the module documents, and any downstream sort/filter
on `vs_score` will misclassify these overlap candidates.

**Fix:** Carry `vs_score` (and consider `via_vs` truthiness) through the annotation:

```python
if v is not None:
    r = dataclasses.replace(r, via_vs=True, vs_rank=v.vs_rank, vs_score=v.vs_score)
```

## Info

### IN-01: `_match_line` assumes every element of `lines` is a string

**File:** `shared/joins_lab.py:600-602`
**Issue:** `rx.search(ln)` will raise `TypeError` if a non-string sneaks into `lines`. In practice
all callers pass `(text or "").split("\n")`, which only yields strings, so this is not currently
reachable — but it is an undocumented precondition on a module-level helper.
**Fix:** Either annotate the precondition in the docstring, or coerce defensively:
`if rx.search(ln if isinstance(ln, str) else str(ln)):`. Low priority — purely a hardening note.

### IN-02: `runtime_checkable` Protocol membership check is name-only

**File:** `shared/joins_lab.py:149-150`, `tests/test_joins_lab.py:161-163`
**Issue:** `@runtime_checkable` makes `isinstance(x, SearchExecutor)` check only that the four method
*names* exist — it does not validate signatures or arity. `test_fake_is_searchexecutor` therefore
asserts less than it appears to: a `FakeSearchExecutor` with wrong-shaped methods would still pass.
This is a known/documented limitation of `runtime_checkable` Protocols, not a defect in this code.
**Fix:** No change required; optionally add a comment in the test noting the isinstance check is
structural-name-only so future readers do not over-trust it.

### IN-03: `apply_cross_side` OR-path note double-counts pages already present in base

**File:** `shared/joins_lab.py:415-462`
**Issue:** The OR note is `f"B matched {len(b_set)} pages · +{added} via other side"`. `added`
counts only newly-synthesized neighbor candidates (those not already in `seen`), which is correct.
There is no bug — flagging only because the AND-path note (`f"B matched {len(b_set)} pages"`,
line 411) and the OR-path note use different phrasings for the same `b_set` count; a downstream UI
that parses or displays these notes side-by-side may want consistent wording. Cosmetic.
**Fix:** Optional — unify the leading clause, e.g. both start with `f"B matched {len(b_set)} pages"`.

---

_Reviewed: 2026-06-03T14:57:07Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
