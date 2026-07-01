---
phase: 132-public-api-dual-mode-api-search-api-parallels
reviewed: 2026-07-01T00:00:00Z
depth: standard
files_reviewed: 5
files_reviewed_list:
  - web/search_api.py
  - shared/fjms_service.py
  - tests/test_search_api_library_mode.py
  - docs/SEARCH_API.md
  - skills/cairo-genizah-research/references/api_contract.md
findings:
  critical: 0
  warning: 3
  info: 2
  total: 5
status: issues_found
---

# Phase 132: Code Review Report

**Reviewed:** 2026-07-01
**Depth:** standard
**Files Reviewed:** 5
**Status:** issues_found

## Summary

Phase 132 adds an optional `library_filter_mode` field (`'include'|'exclude'`) to `FiltersModel`,
shared by both `POST /api/search` and `POST /api/parallels`. The include path is unchanged from
SEED-026. The new exclude path calls `resolve_library_complement_sys_ids` off the event loop via
`run_in_executor`, same pattern as include.

The core logic is correct: the complement helper performs a single-pass O(N) scan of `csv_bank`,
validates input codes against `LIBRARY_CODES`, handles the all-unknown-codes edge case, and the
`_intersect_library_filter` mode normalization is sound (Pydantic ensures only `'include'`/`'exclude'`
or absent; `None or 'include'` covers the absent case correctly).

Three warnings require attention before shipping. The most impactful is a dead-code spy in
`test_include_vs_exclude_disjoint` that renders the key "disjoint" test a tautology — it never
actually validates runtime behavior.

---

## Warnings

### WR-01: `test_include_vs_exclude_disjoint` spy is dead — test only asserts a compile-time constant

**File:** `tests/test_search_api_library_mode.py:303-340`

**Issue:** The test defines `_spy_intersect` and `restrict_sets_seen` (lines 303-310) but never
installs the spy into `web.search_api._intersect_library_filter`. The monkeypatch block patches
`fjms_module.resolve_library_sys_ids` and `fjms_module.resolve_library_complement_sys_ids`, but
`_intersect_library_filter` is imported directly by the endpoint at call time — patching the
fjms_module attributes does intercept the resolver calls correctly, so the 200 status assertions
are real. However, the test's *stated* invariant — "include and exclude result sets must be
disjoint" — is verified only by re-asserting the literal constant `INCLUDE_SYS_IDS &
COMPLEMENT_SYS_IDS == set()` which was already checked at lines 299-300 and is a
compile-time truth. The test NEVER captures what restrict set was actually threaded through the
endpoint for the include vs. exclude case — `restrict_sets_seen` is always empty.

A future regression where include and exclude routes call the SAME helper would pass this test
green, because the disjointness assertion only looks at the stub constants, not at which
stubs were invoked.

**Fix:** Replace the tautological final assertion with checks that verify the correct helper was
called in each case — i.e., record which resolver was invoked, not just that the stubs are
pre-disjoint. Alternatively, restructure to use two separate named assertions:

```python
# After r_include, check include path
assert len(include_calls_seen) == 1, "include mode must call resolve_library_sys_ids"
assert complement_calls_seen_after_include == [], "include mode must NOT call complement"

# After r_exclude, check exclude path
assert len(complement_calls_seen) == 1, "exclude mode must call resolve_library_complement_sys_ids"
assert include_calls_seen_after_exclude == [], "exclude mode must NOT call include"
```

This makes the test a true parity guard rather than a tautology.

---

### WR-02: `resolve_library_complement_sys_ids` returns `set()` when `meta_mgr is None` — silent zero results on exclude path

**File:** `shared/fjms_service.py:3856`

**Issue:** When `meta_mgr is None` (e.g. during server startup before the engine is ready, or in
tests where `state.meta_mgr` was not set), the function returns `set()`. For the include path
(`resolve_library_sys_ids`), an empty return means "no matching IDs" → zero results from a
library-include filter, which is semantically reasonable (the corpus is unavailable). For the
exclude path, an empty return means "no sys_ids survived the complement" → `short_circuit_empty =
True` → zero results. But "exclude CUL when the corpus is unavailable" should logically mean
"cannot determine the complement" — silently returning zero results is a **semantic inversion**
for the exclude use case. A caller who intended `exclude=['CUL']` expecting results from all
other libraries will instead receive an empty response with no error, with no indication that
the corpus was unavailable.

This is not a crash risk (the API already guards `meta_mgr` for the include path in the same
way), and `meta_mgr is None` is highly unlikely at serve time (the mode gate and readiness
checks fire before filter resolution in both endpoints). But the silent behavior diverges from
the documented exclude semantics.

**Fix:** In `resolve_library_complement_sys_ids`, distinguish the `meta_mgr is None` case from
the `not library_codes` case — raise or log at a higher level (WARNING not DEBUG), or return a
sentinel that `_intersect_library_filter` can detect to skip the filter entirely rather than
short-circuiting to zero:

```python
if not library_codes:
    return set()
if meta_mgr is None:
    logger.warning(
        "resolve_library_complement_sys_ids: meta_mgr is None — "
        "cannot compute exclude complement; returning full corpus sentinel"
    )
    # Fail-open for exclude: returning None signals "no filter" to caller.
    # Alternatively, raise so the endpoint returns 503 rather than empty.
    return None  # caller in _intersect_library_filter must handle None
```

And in `_intersect_library_filter`:
```python
if mode == 'exclude':
    lib_ids = await loop.run_in_executor(
        None, _fjms_module.resolve_library_complement_sys_ids, libs, meta_mgr
    )
    if lib_ids is None:
        # meta_mgr unavailable — skip the filter, do not short-circuit to zero
        return restrict_sys_ids
```

---

### WR-03: `validate_filter_values` docstring lists stale key set — `library` and `library_filter_mode` both missing

**File:** `shared/fjms_service.py:1390-1393`

**Issue:** The docstring for `FjmsService.validate_filter_values` states:

> Args:
>     filters: dict with optional keys 'domains', 'authors', 'works',
>              'materials', 'date_from', 'date_to' (D-15 shape, post-
>              Pydantic validation).

The `library` key was added in SEED-026 (Phase 129) and has its own validation block at lines
1499-1508. The new `library_filter_mode` key (Phase 132) is also present in `filters_dict` when
set (since `model_dump(exclude_none=True)` includes it), and is silently ignored. While the
ignore behavior is correct (the mode is Pydantic-validated; no value vocabulary to check), the
docstring does not communicate this intent. A future maintainer reading the docstring would not
know that `library_filter_mode` is an expected passthrough key.

**Fix:** Update the docstring Args section:
```python
    Args:
        filters: dict with optional keys 'domains', 'authors', 'works',
                 'library', 'library_filter_mode', 'materials', 'date_from',
                 'date_to' (D-15 shape, post-Pydantic validation).
                 'library_filter_mode' is intentionally not validated here —
                 it is a Pydantic Literal field with no server-side vocabulary
                 table; it is consumed by _intersect_library_filter.
```

---

## Info

### IN-01: `test_include_mode_is_default_same_as_omitted` complement guard is now always-True

**File:** `tests/test_search_api_library_mode.py:157-163`

**Issue:** The guard `if hasattr(fjms_module, 'resolve_library_complement_sys_ids'):` was written
for Wave 0 (RED phase, before the function existed). Since Plan 02 has now shipped the function,
this guard is always True. The conditional monkeypatch is harmless but leaves dead-guard cruft:
a reader must reason about whether the function could be absent, adding unnecessary cognitive load.

**Fix:** Remove the conditional guard; monkeypatch unconditionally (it is already done with
`raising=False` elsewhere in this file as the clean pattern):
```python
monkeypatch.setattr(
    fjms_module, 'resolve_library_complement_sys_ids',
    lambda codes, mgr: (complement_calls.append(codes) or set()),
)
```

---

### IN-02: Red-phase comments ("RED: ... does not exist yet") are stale after Plan 02 ships

**File:** `tests/test_search_api_library_mode.py` — multiple test docstrings (lines ~121, 152, 194, 216, 237, 291, 346, 393, 424)

**Issue:** Every test docstring opens with "RED: FiltersModel.library_filter_mode field does not
exist yet" or similar. After Plan 02 ships, these notes are misleading — readers will expect the
tests to be red-phase placeholders rather than green-phase regression guards.

**Fix:** Strip the RED-phase annotations from docstrings now that the implementation is live.
The test intent is well-captured by the DMF-11-N labels and the behavioral description; the
build-order note adds no value post-implementation.

---

## Overall Verdict

**The production code (web/search_api.py + shared/fjms_service.py) is correct and safe to
ship.** The complement logic, mode normalization, backward-compat preservation, and both-endpoint
wiring are all sound. The `or 'include'` normalization is safe because Pydantic's `Literal`
constraint and `exclude_none=True` ensure only `'include'`, `'exclude'`, or absent can reach that
line.

**One test has a logic gap** (WR-01): `test_include_vs_exclude_disjoint` does not verify that
the include and exclude HTTP requests actually invoked different resolver paths — it only
re-checks a pre-built constant. This is the most important thing to fix before treating the test
suite as a full regression guard for this feature.

The `meta_mgr is None` silent-zero behavior on exclude (WR-02) is a semantic issue that differs
from the include path's behavior, but it only fires in a pre-initialization state that the server
does not normally expose to clients.

No security regressions were found. Input validation is Pydantic-enforced; the mode field cannot
be injected with arbitrary values; unknown codes continue to return 400; the complement scan runs
off the event loop.

---

_Reviewed: 2026-07-01_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
