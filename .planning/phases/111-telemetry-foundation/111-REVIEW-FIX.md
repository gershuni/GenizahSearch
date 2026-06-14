---
phase: 111-telemetry-foundation
fixed_at: 2026-06-14T15:30:00Z
review_path: .planning/phases/111-telemetry-foundation/111-REVIEW.md
iteration: 1
findings_in_scope: 9
fixed: 9
skipped: 0
status: all_fixed
---

# Phase 111: Code Review Fix Report

**Fixed at:** 2026-06-14T15:30:00Z
**Source review:** `.planning/phases/111-telemetry-foundation/111-REVIEW.md`
**Iteration:** 1

**Summary:**
- Findings in scope: 9 (1 Critical, 5 Warning, 3 Info)
- Fixed: 9
- Skipped: 0

## Fixed Issues

### CR-01: Scrubber does not recurse into `$set` / `$set_once` — nested PII reaches the transport

**Files modified:** `desktop/telemetry.py`, `tests/test_telemetry_review_fixes.py`
**Commit:** `9a26af85`
**Applied fix:** Added `_scrub_value(v)` function that recurses into dict/list/tuple
values, applying banned-key drop and path/Hebrew/length redaction at every nesting level.
`_scrub_props()` now delegates to `_scrub_value()` for each value. Added `_scrub_value`
to `__all__`. Added 6 regression tests covering email/path/Hebrew in `$set`, `$set_once`,
and list values, plus the full pipeline `track()` exploit scenario from the finding.

### WR-01: Path-redaction regex runs on the uncapped value

**Files modified:** `desktop/telemetry.py`, `tests/test_telemetry_review_fixes.py`
**Commit:** `9a26af85`
**Applied fix:** In `_scrub_value()`, the 500-char cap (`v = v[:500]`) now runs BEFORE
`_PATH_RE.sub()` and the Hebrew check, bounding worst-case regex cost. Added 2 regression
tests (direct `_scrub_value` on a 100,000-char string, and nested string in `$set`).

### WR-02: Opt-out does not reset in-memory identity state

**Files modified:** `desktop/telemetry.py`, `tests/test_telemetry_review_fixes.py`
**Commit:** `9a26af85`
**Applied fix:** In `set_consent(False)`:
(1) `updates[IDENTIFIED_USER_KEY] = None` is written to config (clean privacy boundary;
CONSENT-06 only requires retaining `telemetry_install_id`).
(2) After drain, `_identified = False` and `_current_distinct_id = _install_id` under
`_state_lock` so re-opt-in starts anonymous without stale identity. Updated docstring.
Added 2 regression tests: full opt-in/identify/opt-out/re-opt-in cycle, and config-key
clear verification.

### WR-03: `__main__` self-test persistently mutates real consent and install-id

**Files modified:** `desktop/telemetry.py`
**Commit:** `9a26af85`
**Applied fix:** The `__main__` self-test block now snapshots `prior_enabled = is_enabled()`,
toggles `_enabled = True` in-memory only (no `set_consent(True)` call, no config.pkl write),
runs `_wire_transport_config()` to apply the env key, calls `run_selftest()`, then restores
`_enabled = prior_enabled` in a `finally` block. Config is left untouched.

### WR-04: Non-atomic identity snapshot in `_emit`

**Files modified:** `desktop/telemetry.py`
**Commit:** `9a26af85`
**Applied fix:** `_emit()` now reads both `_identified` and `_current_distinct_id` (as
`effective_id`) in a single `with _state_lock:` block, eliminating the two-lock window
where an identify/reset on another thread could mislabel an event with mismatched
identity state and distinct_id.

### WR-05: Embedded placeholder key is truthy — events POST with junk api_key

**Files modified:** `desktop/telemetry.py`, `tests/test_telemetry_review_fixes.py`
**Commit:** `9a26af85`
**Applied fix:** In `_wire_transport_config()`, after resolving the key from env or default,
a check `if key == _TELEMETRY_KEY_DEFAULT: key = None` converts the placeholder to None
before calling `set_capture_api_key(key)`. The drain loop already skips POST when
`not api_key`, so events are dropped locally rather than sent with the junk sentinel key.
Real `phc_...` keys pass through unchanged. Added 3 regression tests.

### IN-01: `_PATH_RE` filename clause misses extensions outside 2-4 chars

**Files modified:** `desktop/telemetry.py`
**Commit:** `9a26af85`
**Applied fix:** The bare-filename clause was widened from `\w{2,4}\b` to
`[A-Za-z]\w{0,7}\b` (letter-started extension, 1-8 total chars). This covers `.markdown`,
`.sqlite3`, `.config` and other longer extensions while avoiding false positives on version
strings like `8.0.0` (which have digit-only components). Verified: existing
`test_track_adds_base_props` passes with `app_version='8.0.0'` not redacted.

Note: this finding is classified as IN-01 (Info) but the fix is defense-in-depth and
correctness was verified by the test suite.

### IN-02: `_emit`/`identify` allow callers to override `$process_person_profile`

**Files modified:** `desktop/telemetry.py`, `tests/test_telemetry_review_fixes.py`
**Commit:** `9a26af85`
**Applied fix:** In `_emit()`, `merged['$process_person_profile'] = identified` is applied
a second time AFTER `merged.update(props)`, overriding any caller-supplied value. Anonymous
users cannot force `True` via track() kwargs; `identify()` still sets it correctly through
its own direct path (not via `_emit`). Added 2 regression tests.

### IN-03: `track()` resolves raw string via `str(event)` before allowlist check — needs guard comment

**Files modified:** `desktop/telemetry.py`
**Commit:** `9a26af85`
**Applied fix:** Added inline comments at the `str(event)` call in both `track()` and
`track_performance()` noting that the result is immediately re-validated against
`_VALID_EVENT_VALUES` (the fixed DesktopEvent registry, PRIV-06) and that Phase 113-115
authors must not bypass this check. No logic change needed — the existing validation
is correct; the fix is documentation only.

---

## Test Results

**Total tests:** 97 passed, 0 failed, 0 errors
- Existing telemetry tests: 82 passed
- New regression tests (test_telemetry_review_fixes.py): 15 passed

**Ruff lint:** `All checks passed!` on `desktop/telemetry.py`,
`shared/posthog_server.py`, and all `tests/test_telemetry_*.py` files.

**PRIV-03 AST guard (test_telemetry_no_direct_posthog.py):** Passes — no posthog
SDK imports introduced.

---

_Fixed: 2026-06-14T15:30:00Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
