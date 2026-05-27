---
phase: 98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening
plan: 04
subsystem: resilience
tags: [nli, iiif, circuit-breaker, timeout, puzzle, host-conditional]

# Dependency graph
requires:
  - phase: 98-02
    provides: "shared/nli_circuit_breaker.py module-level singleton (is_open / record_failure / record_success + env-driven NLI_CONNECT_TIMEOUT / NLI_IIIF_READ_TIMEOUT / NLI_IMAGE_READ_TIMEOUT constants)"
provides:
  - "PuzzleImageService._fetch_iiif_image guarded by NLI circuit breaker (D-19)"
  - "PuzzleImageService._fetch_direct_url host-conditional breaker scoping (D-20)"
  - "web/pages/puzzle.py::_resolve_folios NLI manifest fetch guarded by breaker (D-21)"
  - "Bounded timeouts on all NLI puzzle paths (was 30s images / 15s manifest -> env-driven 5s read / 3s connect)"
  - "Test invariant pinning Cambridge/Manchester/Oxford failures do NOT trip the NLI breaker"
affects: [98-05, 98-06, puzzle, image-resolution]

# Tech tracking
tech-stack:
  added: []  # No new deps; consumes existing shared.nli_circuit_breaker
  patterns:
    - "host-conditional breaker scoping via urlparse(url).netloc evaluated AFTER URL construction"
    - "NLI image vs JSON-manifest timeout separation (IMAGE_READ_TIMEOUT for tiles, IIIF_READ_TIMEOUT for manifests)"
    - "Cambridge/Manchester/Oxford retain non-NLI 30s timeout — they are not the failure mode"
    - "200-with-empty-data falls through without tripping breaker (D-07 negative-cache semantics)"
    - "JSON-parse exceptions trapped at innermost catch-all WITHOUT calling record_failure (T-98-04-06)"

key-files:
  created:
    - tests/test_puzzle_nli_breaker_integration.py
  modified:
    - shared/puzzle_image_service.py
    - web/pages/puzzle.py

key-decisions:
  - "D-19 closed: _fetch_iiif_image unconditionally guarded; timeout 30s -> (NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)"
  - "D-20 closed: _fetch_direct_url uses urlparse(url).netloc to compute is_nli_host; only iiif.nli.org.il and rosetta.nli.org.il trip the breaker; all other hosts keep their 30s timeout and do NOT call the breaker"
  - "D-21 closed: _resolve_folios NLI manifest fetch uses NLI_IIIF_READ_TIMEOUT (5s, JSON manifest) NOT NLI_IMAGE_READ_TIMEOUT; on breaker open, falls through to images_ext fallback rather than returning empty"
  - "D-13 partial: 3/10 call sites guarded by this plan; remaining sites covered by parallel plans 98-03 (web/api.py) and 98-05 (genizah_core.py)"
  - "urlparse evaluated AFTER URL construction (T-98-04-03 mitigation): callers cannot smuggle a different host via the '/full/' short-circuit"

patterns-established:
  - "Host-conditional breaker scoping: shared breaker module + per-call-site host predicate. Mitigates cross-provider blast radius (Cambridge outage does NOT degrade NLI UX, and vice versa)."
  - "Manifest vs image timeout separation: NLI_IIIF_READ_TIMEOUT for JSON metadata fetches, NLI_IMAGE_READ_TIMEOUT for binary tile fetches. Manifest is ~1KB, image is 100KB-2MB; different read budgets apply."
  - "Static source-audit tests use regex form `pattern(?![\\d.])` instead of literal substring match — pin call-site code form without false-positives from docstring/comment prose."

requirements-completed: [D-13, D-19, D-20, D-21]

# Metrics
duration: 9m
completed: 2026-05-25
---

# Phase 98 Plan 04: Puzzle NLI Breaker Wiring Summary

**Shared NLI circuit breaker wired into 3 puzzle-subsystem call sites with host-conditional scoping — Cambridge/Manchester/Oxford failures cannot trip the NLI breaker (pinned by 4 cross-provider isolation tests).**

## Performance

- **Duration:** ~9 min (8m 33s)
- **Started:** 2026-05-25T14:35:24Z
- **Completed:** 2026-05-25T14:43:57Z
- **Tasks:** 3
- **Files modified:** 3 (2 modified + 1 new test file)

## Accomplishments

- **D-19:** `PuzzleImageService._fetch_iiif_image` unconditionally guarded by `is_open()`; hard-coded `timeout=30` replaced by `(NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)` env-driven tuple (defaults: 3s connect / 5s read); typed `Timeout` / `ConnectionError` / `5xx` / `429` increment the breaker; 200 OK records success. Bare `except Exception` removed in favour of specific exception classes (RESEARCH Pitfall 7).
- **D-20:** `PuzzleImageService._fetch_direct_url` now computes `is_nli_host` via `urlparse(url).netloc in ('iiif.nli.org.il', 'rosetta.nli.org.il')` AFTER URL construction. NLI hosts go through the breaker with the bounded timeout; **all other hosts (Cambridge/Manchester/Oxford and any future IIIF provider) retain their existing `timeout=30` and do NOT touch the breaker.** This is the most critical invariant of this plan.
- **D-21:** `_resolve_folios` NLI IIIF-manifest fetch (`web/pages/puzzle.py:~1989`) guarded by the breaker; hard-coded `timeout=15` replaced by `(NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT)` tuple (defaults: 3s connect / 5s read). Manifest is JSON, NOT image bytes — uses `IIIF_READ_TIMEOUT` (5s) not `IMAGE_READ_TIMEOUT` (5s — same default but env-separated). When the breaker is open, the NLI block is skipped entirely and the function falls through to the existing `images_ext` fallback (preserves last-resort path).
- **30 integration tests** in `tests/test_puzzle_nli_breaker_integration.py` across 3 classes (`TestFetchIiifImageBreakerGuard`, `TestFetchDirectUrlHostConditional`, `TestStaticSourceAudits`) — all green. Combined Wave 1+2+3 slice (`test_nli_circuit_breaker.py + test_posthog_server.py + test_puzzle_nli_breaker_integration.py`): 77/77 passing.

## Task Commits

Each task was committed atomically (worktree branch `phase-98-nli-resilience`, base `646d3fa4`):

1. **Task 1: D-19 + D-20 wiring in `shared/puzzle_image_service.py`** — `3fe9e4fb` (feat)
2. **Task 2: D-21 wiring in `web/pages/puzzle.py::_resolve_folios`** — `4640d03c` (feat)
3. **Task 3: integration tests `tests/test_puzzle_nli_breaker_integration.py`** — `f7e7f974` (test)

_Plan metadata commit (this SUMMARY.md) follows. STATE.md / ROADMAP.md intentionally NOT modified per parallel-execution instructions._

## Files Created/Modified

- **`shared/puzzle_image_service.py`** (modified):
  - Added `from urllib.parse import urlparse` and `from shared.nli_circuit_breaker import (is_open, record_failure, record_success, NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)` aliased with `_nli_` prefix.
  - `_fetch_direct_url` (≈line 157): host-conditional `is_nli_host` computation; ternary `timeout = (NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT) if is_nli_host else 30`; breaker calls scoped to `is_nli_host == True`; typed exception handlers + catch-all for malformed-URL safety.
  - `_fetch_iiif_image` (≈line 230): unconditional `if _nli_circuit_is_open(): return None` short-circuit; bounded timeout tuple; typed exception handlers with breaker increment; bare `except Exception` removed.

- **`web/pages/puzzle.py`** (modified):
  - Added breaker import block under the existing `from web.state import state` import.
  - `_resolve_folios` (≈line 1989): wrapped the NLI manifest `try` block in `if not _nli_circuit_is_open():`; replaced `timeout=15` with `(NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT)`; added typed `_requests.exceptions.Timeout` / `ConnectionError` handlers that increment the breaker; preserved innermost `except Exception` for JSON-decode errors WITHOUT incrementing the breaker (T-98-04-06). Success path with non-empty `fl_ids` calls `_nli_record_success(path='puzzle_resolve_folios')`.

- **`tests/test_puzzle_nli_breaker_integration.py`** (created — 499 lines):
  - `TestFetchIiifImageBreakerGuard` (7 tests): short-circuit when open, success resets counter, typed `Timeout` / `ConnectionError` / `5xx` / `429` record failures, bounded timeout tuple pinned.
  - `TestFetchDirectUrlHostConditional` (11 tests): NLI hosts (`iiif.nli.org.il` AND `rosetta.nli.org.il`) short-circuit + record; **Cambridge/Manchester/Oxford fetch even when breaker open AND do NOT increment the breaker on timeout/5xx/connection_error** (the core D-20 invariant pinned 3 ways across 3 providers); malformed URL with empty netloc skips breaker (T-98-04-05).
  - `TestStaticSourceAudits` (12 tests): source-level pins for breaker import, no code-form `timeout=30` in `_fetch_iiif_image`, no code-form `timeout=15` in `web/pages/puzzle.py`, `is_nli_host` present, NLI host tuple lists both `iiif.nli.org.il` and `rosetta.nli.org.il`, typed exception handler count, success/failure record paths wired, 3-call-site breaker-check count.

## Decisions Made

- **`urlparse` placed at module level** (not inside `_fetch_direct_url`): the plan suggested inline import for clarity; I lifted it to module-level alongside the other stdlib imports. Equivalent semantics, slightly cleaner.
- **`else 30` ternary rather than `timeout=30` literal**: the non-NLI fallback uses `timeout = (NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT) if is_nli_host else 30`, then `requests.get(..., timeout=timeout, ...)`. Result: the literal call-site form `timeout=30` is GONE from code (it's the ternary `else 30` branch). Static-audit regex `r'timeout=30(?![\d.])'` finds 0 matches.
- **`_resolve_folios` breaker-open behaviour:** skip NLI fetch and fall through to `images_ext` fallback rather than returning empty list immediately. This preserves the function's downstream path — a manuscript whose NLI manifest is unreachable but whose `images_ext` (Friedberg-supplied) is populated will still resolve folios.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Test Bug] Plan's literal-substring tests would false-positive on docstring text**

- **Found during:** Task 3 (running new tests)
- **Issue:** The plan specified `assert 'timeout=15' not in src` and `assert 'timeout=30' not in body`. These literal substring matches incorrectly fail because:
  - `web/pages/puzzle.py` contains unrelated `timeout=15.0` calls in `ui.run_javascript(...)` (NiceGUI client-side JS execution timeout, NOT HTTP requests). The substring `timeout=15` appears inside `timeout=15.0`.
  - `shared/puzzle_image_service.py` contains the doc-comment "replaces hard-coded `timeout=30`" inside the D-19 inline note, which causes the substring match to fail.
  - The actual D-19/D-21 invariant is about call-site CODE form, not prose mentions.
- **Fix:** Tightened both static-audit tests to use `re.compile(r'timeout=N(?![\d.])')` — negative-lookahead excludes the following character being a digit or `.`. This matches `timeout=15)` / `timeout=15,` / `timeout=15<whitespace>` (CODE form) but skips `timeout=15.0` (JS form) and skips prose mentions of `timeout=30` followed by a period.
- **Files modified:** `tests/test_puzzle_nli_breaker_integration.py` (`test_puzzle_service_no_timeout_30_in_nli_method`, `test_puzzle_service_timeout_30_only_for_non_nli`, `test_puzzle_page_no_int_timeout_15_for_http`)
- **Verification:** All 30 tests now pass; the regex correctly distinguishes call-site code form from prose. Documented in test docstring with explicit rationale.
- **Committed in:** `f7e7f974` (Task 3 commit)

---

**Total deviations:** 1 auto-fixed (Rule 1 — test correctness)
**Impact on plan:** Test-only fix. Production code unchanged. The deviation strengthens the test (call-site form pinned precisely instead of substring-matching). The D-19, D-20, D-21 invariants are unaffected — the *intent* of the plan's tests is preserved with greater fidelity.

## Issues Encountered

None during execution beyond the test-substring issue documented as a deviation above.

## Verification Evidence

```
$ python -m py_compile shared/puzzle_image_service.py web/pages/puzzle.py
COMPILE OK

$ python -m pytest tests/test_puzzle_nli_breaker_integration.py -x --tb=short
============================= 30 passed in 0.45s ==============================

$ python -m pytest tests/test_nli_circuit_breaker.py tests/test_posthog_server.py \
                   tests/test_puzzle_nli_breaker_integration.py -x --tb=short
============================= 77 passed in 2.62s ==============================

$ # Breaker-check audit
shared/puzzle_image_service.py: is_open checks=2, code-form timeout=30=0, code-form timeout=15=0
web/pages/puzzle.py:            is_open checks=1, code-form timeout=30=0, code-form timeout=15=0
```

All Plan 98-04 success criteria met:

- [x] 3 puzzle call sites guarded (2 in `shared/puzzle_image_service.py`, 1 in `web/pages/puzzle.py`)
- [x] NLI image fetches use `(NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)`
- [x] NLI JSON manifest fetch uses `(NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT)`
- [x] Non-NLI hosts (Cambridge/Manchester/Oxford) retain 30s timeout AND do NOT touch breaker (4 isolation tests pin this)
- [x] Every NLI success / typed-failure path emits the right breaker call
- [x] 404 / 200-empty / JSON-parse errors do NOT trip breaker (D-07 + T-98-04-06)
- [x] Combined Wave 1+2+3 slice green: 77/77

## File Overlap Check

This plan ran in parallel with 98-03 (web/api.py) and 98-05 (genizah_core.py). File modifications:

- 98-04 modified: `shared/puzzle_image_service.py`, `web/pages/puzzle.py`, `tests/test_puzzle_nli_breaker_integration.py` — disjoint from 98-03 / 98-05 file lists.

No conflicts expected at merge.

## Next Phase Readiness

- D-19, D-20, D-21 closed. D-13 partially closed (3/10 sites).
- Plan 98-06 (production canary) can include puzzle endpoints in its smoke tests:
  - Puzzle add-fragment on NLI manuscript (exercises `_resolve_folios` + `_fetch_iiif_image`)
  - Puzzle add-fragment on Cambridge manuscript (exercises `_fetch_direct_url` non-NLI branch; should be unaffected by NLI degradation)
- Static source-audit pattern (`re.compile(r'timeout=N(?![\d.])')`) is now established for future cross-provider timeout invariants.

## Self-Check

- [x] `shared/puzzle_image_service.py` exists (modified) — confirmed by `git log --oneline -- shared/puzzle_image_service.py` showing `3fe9e4fb`.
- [x] `web/pages/puzzle.py` exists (modified) — confirmed by `git log --oneline -- web/pages/puzzle.py` showing `4640d03c`.
- [x] `tests/test_puzzle_nli_breaker_integration.py` exists (new file, 499 lines) — confirmed by `git log --oneline -- tests/test_puzzle_nli_breaker_integration.py` showing `f7e7f974`.
- [x] Commits `3fe9e4fb`, `4640d03c`, `f7e7f974` all on branch `phase-98-nli-resilience` (verified via `git log --oneline`).

---
*Phase: 98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening*
*Plan: 04 (Wave 3, parallel-safe with 98-03 + 98-05)*
*Completed: 2026-05-25*
