---
phase: 115-performance-metrics
reviewed: 2026-06-16T09:30:00Z
depth: standard
files_reviewed: 6
files_reviewed_list:
  - desktop/telemetry.py
  - desktop/my_library_tab.py
  - genizah_app.py
  - gui_threads.py
  - tests/test_telemetry_phase115.py
  - tests/test_no_dynamic_telemetry_strings.py
findings:
  critical: 0
  warning: 4
  info: 5
  total: 9
status: issues_found
---

# Phase 115: Code Review Report

**Reviewed:** 2026-06-16T09:30:00Z
**Depth:** standard
**Files Reviewed:** 6
**Status:** issues_found

## Summary

Reviewed the Phase 115 delta (perf telemetry) at `c9571f0d..HEAD` across the
telemetry chokepoint, the four search worker threads, the GUI orchestration
(`genizah_app.py`), and the My Library indexing producers. The 17 Phase 115
tests pass locally (`GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`).

The privacy and consent invariants hold up well under adversarial reading:
no posthog SDK is used, accumulation is consent-gated, `set_consent(False)`
clears the in-memory accumulator (CONSENT-08), the flush is per-session and
aggregate-only, mode/corpus/operation_kind are normalized to fixed allowed
sets before becoming dict keys, raw per-search result counts and duration
lists never leave memory (only buckets + percentile stats are emitted), and
timing uses `perf_counter`/`monotonic`. The D-17 AST guard is non-vacuous and
the producer call sites use only literal constants. No BLOCKER-class defects
(no crash, no privacy leak, no data loss) were found.

Four WARNING-class defects degrade the **usefulness** of the telemetry rather
than its safety:
1. The perf summary's `session_id` is the *install identity*, not the
   per-process session UUID that `session_start`/`session_end` carry — so the
   summary cannot be joined to its session, defeating the field's documented
   purpose.
2. Most LAB-mode search/composition mode values normalize to `'unknown'`
   because they are missing from `_PERF_ALLOWED_MODES`.
3. Every LAB-rebuild indexing event reports `doc_count_bucket='0'`,
   indistinguishable from a genuinely empty rebuild.
4. One production-guard assertion is vacuous due to operator precedence.

## Warnings

### WR-01: perf summary `session_id` will not join to session_start / session_end

**File:** `desktop/telemetry.py:1578-1582`, cross-ref `genizah_app.py:3634, 27005`
**Issue:** `_flush_perf_summary` sets the summary's `session_id` from the
persistent install identity:
```python
with _state_lock:
    sid = _current_distinct_id or _install_id or ''
```
The inline comment claims this is "the same value SESSION_END uses." It is not.
`SESSION_START` and `SESSION_END` both emit `session_id=self._session_id`, where
`self._session_id = uuid.uuid4().hex` is minted fresh **per process** in the
startup coordinator (`genizah_app.py:3634`). `_current_distinct_id`/`_install_id`
is the *persistent anonymous install distinct_id* (stable across all sessions on
that install). These are two different identifiers, so the documented goal of
REVIEWS finding 4 ("join to session_start/end") is impossible: every session's
perf summary carries the same install-level value, which equals the PostHog
`distinct_id`, not the session UUID. The test only asserts non-emptiness
(`test_telemetry_phase115.py:149`), so it passes despite the semantic error.
**Fix:** Plumb the real per-process session UUID into the flush. Pass it from the
GUI flush helpers (which have `self._session_id`) rather than deriving it inside
the module, e.g.:
```python
# genizah_app.py flush call sites
telemetry.flush_perf_unconditionally(session_id=getattr(self, '_session_id', ''))
# and flush_perf_if_due(session_id=...) likewise
# telemetry._flush_perf_summary then uses the passed session_id verbatim
```
and correct the misleading comment at `telemetry.py:1578`.

### WR-02: LAB-mode search/composition modes collapse to 'unknown'

**File:** `desktop/telemetry.py:1391-1395`, cross-ref `genizah_app.py:17536, 23031`
**Issue:** Search mode values are built as `f'lab_{_mode_key}'` for the eight
search modes (`genizah_app.py:17536`) and `f'lab_{_comp_mode_key}'` for the
three composition modes (`genizah_app.py:23031`). The producer can therefore
emit `lab_keyword`, `lab_variants`, `lab_responsa`, `lab_fuzzy`, `lab_regex`,
`lab_title`, `lab_shelfmark`, `lab_pgp_tags`, `lab_comp_exact`,
`lab_comp_variants`, `lab_comp_fuzzy`. But `_PERF_ALLOWED_MODES` lists only
`lab_variants` and `lab_comp_exact` among the `lab_*` family. Every other
LAB-mode search normalizes to `'unknown'` via `_normalize_mode`, merging
distinct LAB modes into one undifferentiated bucket. This is a data-quality
defect: LAB-mode performance (a primary motivation for this telemetry) is
largely unattributable by mode.
**Fix:** Add the full `lab_*` family to the allowlist so the keys round-trip:
```python
_PERF_ALLOWED_MODES = frozenset({
    'keyword', 'variants', 'responsa', 'fuzzy', 'regex', 'title', 'shelfmark', 'pgp_tags',
    'comp_exact', 'comp_variants', 'comp_fuzzy',
    'lab_keyword', 'lab_variants', 'lab_responsa', 'lab_fuzzy', 'lab_regex',
    'lab_title', 'lab_shelfmark', 'lab_pgp_tags',
    'lab_comp_exact', 'lab_comp_variants', 'lab_comp_fuzzy',
})
```
(All `lab_*` values are still fixed enum strings, so privacy is unaffected.)

### WR-03: LAB-rebuild indexing event always reports doc_count_bucket='0'

**File:** `desktop/my_library_tab.py:794-799, 1233-1248`
**Issue:** `LabRebuildWorker.run` emits `finished_signal.emit(elapsed_ms, 0)`
unconditionally (the rebuild does not return a doc count). In
`_on_lab_rebuild_finished`, `total_docs == 0` maps to `doc_count_bucket='0'`,
which is the **same** bucket a genuinely empty/zero-doc rebuild would produce.
Downstream analysis cannot tell "we don't know the count" from "rebuilt 0
documents." The code comment acknowledges the 0 sentinel but the consumer has
no way to distinguish it.
**Fix:** Use a distinct sentinel bucket for "unknown" rather than overloading
`'0'`, e.g. emit `doc_count_bucket='unknown'` when `total_docs == 0` on the
lab-rebuild path, or obtain the real doc count from the rebuilt index. If
`'unknown'` is used, add it to the documented `doc_count_bucket` value set.

### WR-04: vacuous path-leak assertion in perf-bucket test

**File:** `tests/test_telemetry_phase115.py:262`
**Issue:**
```python
assert ':\\' not in payload_repr and '/' not in payload_repr or True
```
Python precedence parses this as `(A and B) or True`, which is **always True**.
The intended "no path-like string in the payload" check is dead — it can never
fail, so a real path leak in the serialized summary would pass this test
silently. (The test comments even call it "best-effort," but as written it is
no effort.)
**Fix:** Remove the tautological `or True` and assert the real condition, or
drop the line if path absence is already covered by the scrubber unit tests:
```python
assert ':\\' not in payload_repr, "Windows path leaked into perf summary payload"
assert '/' not in payload_repr,  "POSIX path-like string leaked into perf summary payload"
```
(Note: bucket labels like `'100+'` contain no `/` so this would still pass for
legitimate payloads.)

## Info

### IN-01: `corpus_scope='unknown'` counts are silently dropped from the summary

**File:** `desktop/telemetry.py:1516, 1571-1573`
**Issue:** `accumulate_performance` tracks four corpus buckets
(`genizah`/`local`/`all`/`unknown`), but `_flush_perf_summary` only serializes
`corpus_genizah`/`corpus_local`/`corpus_all`. Any search whose corpus
normalized to `'unknown'` contributes to `count`/percentiles but its corpus
attribution vanishes (sum of `corpus_*` may be less than `count`).
**Fix:** Either add `'corpus_unknown'` to the emitted stats, or document that
`count` minus the three corpus columns equals the unknown-corpus tally.

### IN-02: Hebrew-range assertion in privacy test misses Presentation Forms

**File:** `tests/test_telemetry_phase115.py:257-261`
**Issue:** The payload Hebrew scan only checks `0x0590 <= code <= 0x05FF`, but
the production scrubber's `_HEBREW_TEXT_RE` also covers Hebrew Presentation
Forms (`U+FB1D-U+FB4F`). The test would not catch a Presentation-Forms leak.
**Fix:** Extend the test range to include `0xFB1D <= code <= 0xFB4F` to mirror
the scrubber's coverage.

### IN-03: `_perf_sample_counter` not reset by `_flush_perf_summary` (only by clear)

**File:** `desktop/telemetry.py:1528-1599` vs `1372-1385`
**Issue:** `_flush_perf_summary` resets `_perf_accumulator` and
`_perf_last_flush_time` but not `_perf_sample_counter`; only
`_clear_perf_accumulator` zeroes all three. This is intentional (continuous
sampling across flush windows) and not a bug, but the asymmetry between the two
reset paths is a latent foot-gun for future maintainers.
**Fix:** Add a one-line comment at the flush reset noting that
`_perf_sample_counter` is deliberately preserved so the sampling cadence is
continuous across windows.

### IN-04: misleading "Pitfall 2 — must be first line" comment in worker threads

**File:** `gui_threads.py:102, 154, 217, 285`; `desktop/my_library_tab.py:740, 793`
**Issue:** Each `t0 = time.perf_counter()` / `_t0 = time.monotonic()` is
commented "must be first line (Pitfall 2)," but in three of the worker `run()`
bodies the timer is preceded by `_prevent_sleep()` (gui_threads) or set inside
the `try` after the docstring. The intent (measure as much work as possible) is
met, but the "first line" wording is inaccurate and could mislead a future edit
into reordering for literal compliance.
**Fix:** Reword to "before any search/index work begins" rather than "first
line."

### IN-05: duplicated bucket logic across three sites risks drift

**File:** `desktop/telemetry.py:1550-1559`, `genizah_app.py:3278-3290`,
`desktop/my_library_tab.py:1236-1241, 1888-1893`
**Issue:** The 0 / 1-9 / 10-99 / 100+ bucket boundaries are reimplemented in at
least three places (the perf-summary result buckets, `_telemetry_result_bucket`,
and the two `doc_count_bucket` ternaries). The telemetry.py comment explicitly
says "keep in sync" with `genizah_app._telemetry_result_bucket` but cannot
import it (circular import). They agree today, but any future change to the
scheme must be made in lockstep across files.
**Fix:** Extract a single bucket helper into `shared/` (importable by both
`desktop/` and the top-level GUI without a cycle) and call it from all sites.

---

_Reviewed: 2026-06-16T09:30:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
