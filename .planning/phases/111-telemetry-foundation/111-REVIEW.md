---
phase: 111-telemetry-foundation
reviewed: 2026-06-14T10:02:09Z
depth: standard
files_reviewed: 8
files_reviewed_list:
  - desktop/telemetry.py
  - shared/posthog_server.py
  - tests/test_telemetry_allowlist.py
  - tests/test_telemetry_consent_gate.py
  - tests/test_telemetry_identity.py
  - tests/test_telemetry_no_direct_posthog.py
  - tests/test_telemetry_posthog_server_ext.py
  - tests/test_telemetry_scrubbing.py
findings:
  critical: 1
  warning: 5
  info: 3
  total: 9
status: issues_found
---

# Phase 111: Code Review Report

**Reviewed:** 2026-06-14T10:02:09Z
**Depth:** standard
**Files Reviewed:** 8
**Status:** issues_found

## Summary

Phase 111 builds the opt-in PostHog telemetry foundation for the desktop app:
`shared/posthog_server.py` gains six backward-compatible transport/flush helpers, and
`desktop/telemetry.py` is the sole consent-gated chokepoint with a structural scrubber,
property allowlist, and a fixed `DesktopEvent` enum. The consent gate, no-throw contracts,
opt-out drain, install-id lifecycle, identity mechanism, and the PRIV-03 AST guard are all
implemented competently and well-tested for the cases the tests exercise.

However, the central privacy invariant — "make it impossible for free-form event names,
file paths, Hebrew/search content, or PII to reach the transport" — has a **proven hole**:
the scrubber redacts only top-level string values and never recurses into the allowlisted
`$set` / `$set_once` dict-valued keys. A single public `track(..., **{'$set': {...}})` call
leaks email, Windows paths, and Hebrew text straight to the queue. This is the one BLOCKER.

Secondary concerns: the scrubber runs a backtracking-prone regex on the **uncapped** value
(length cap applied afterward), so a large string value can hang the crash path for tens of
seconds; opt-out does not reset in-memory identity state; and the `__main__` self-test
persistently mutates real consent. None of these block the BLOCKER's severity but all should
be fixed before producers are wired in Phases 112-115.

## Structural Findings (fallow)

No `<structural_findings>` block was provided with this review. Section intentionally empty.

## Narrative Findings (AI reviewer)

## Critical Issues

### CR-01: Scrubber does not recurse into `$set` / `$set_once` — nested PII reaches the transport

**File:** `desktop/telemetry.py:188-207` (`_scrub_props`), allowlist at `210-230`, exploited via `track()` `419-447`

**Issue:** `$set` and `$set_once` are allowlisted top-level keys (lines 217-218) whose values
are *dicts*. `_scrub_props` only inspects values with `isinstance(v, str)` (line 201); any
dict value bypasses both the banned-key check (which only runs on the top-level key, not nested
keys) and the path/Hebrew/length redaction entirely. Because `track(event, **props)` forwards
arbitrary caller kwargs and `$set`/`$set_once` survive `_validate_props`, a caller can smuggle
PII, file paths, and Hebrew search content past the chokepoint. This directly violates the
phase invariant ("impossible for ... PII to reach the transport", PRIV-01/PRIV-02/IDENT-03).

Proven empirically (consent True, real chokepoint):

```python
tel.track(DesktopEvent.SELFTEST, **{'$set': {'email': 'leak@x.com',
                                             'p': r'C:\secret\f.pdf',
                                             'h': 'תשובות'}})
# enqueued payload properties:
# {'platform': 'desktop', 'app_version': '8.0.0', '$process_person_profile': False,
#  '$set': {'email': 'leak@x.com', 'p': 'C:\\secret\\f.pdf', 'h': 'תשובות'}}
```

All three forbidden classes (email PII, Windows path, Hebrew content) reach `enqueue_event`
unredacted. The same hole exists for any future identify-style `$set` payload.

**Fix:** Recurse the scrubber into dict values, and apply the banned-key + value redaction at
every level. Minimal change:

```python
def _scrub_value(v):
    if isinstance(v, str):
        v = _PATH_RE.sub('[REDACTED]', v)
        if _HEBREW_TEXT_RE.search(v):
            return '[REDACTED]'
        return v[:500]
    if isinstance(v, dict):
        return {k: _scrub_value(val) for k, val in v.items()
                if not _is_banned_key(k)}
    if isinstance(v, (list, tuple)):
        return [_scrub_value(x) for x in v]
    return v

def _scrub_props(props: dict) -> dict:
    out: dict = {}
    for k, v in props.items():
        if _is_banned_key(k):
            continue
        out[k] = _scrub_value(v)
    return out
```

Additionally, consider dropping `$set`/`$set_once` from `_ALLOWED_PROPS` until Phase 113+
actually needs them (Phase 111 ships zero producers), or restricting their permitted nested
keys to an explicit sub-allowlist.

## Warnings

### WR-01: Path-redaction regex runs on the uncapped value — multi-second hang on the crash path

**File:** `desktop/telemetry.py:201-205`

**Issue:** The 500-char length cap (`v = v[:500]`, line 205) is applied **after**
`_PATH_RE.sub('[REDACTED]', v)` (line 202). `_PATH_RE` contains backtracking-prone alternations
(`/\S{3,}` and `\S+\.\w{2,4}\b`). Measured `sub()` cost on a single string value:

| value length | no-dot | many-dots |
|--------------|--------|-----------|
| 10,000       | 0.13s  | 0.68s     |
| 100,000      | 14.8s  | 73.9s     |

The module comment (line 141) explicitly states "crash hooks must be fast" because
`_scrub_props` runs inside `track_error()` (the CRASH path) and `_emit`. A large string value
(e.g. a long `context` label or a `traceback_scrubbed` blob in Phase 113) would stall the crash
hook for tens of seconds, undermining the bounded-flush/`_flush_before_exit` design.

**Fix:** Cap length first, then run the redaction passes:

```python
if isinstance(v, str):
    v = v[:500]                      # cap BEFORE the regex
    v = _PATH_RE.sub('[REDACTED]', v)
    if _HEBREW_TEXT_RE.search(v):
        v = '[REDACTED]'
```

### WR-02: Opt-out does not reset in-memory identity state — re-opt-in resumes under the prior identified user

**File:** `desktop/telemetry.py:366-377` (`set_consent` opt-out branch)

**Issue:** On opt-out, `set_consent(False)` sets `_enabled=False`, drains the queue, and clears
the transport default distinct_id — but it never resets `_current_distinct_id` or `_identified`,
and never clears `IDENTIFIED_USER_KEY` in config. If a user who previously called `identify()`
opts out and then opts back in within the same session, `_identified` is still `True` and
`_current_distinct_id` is still the prior Supabase user id, so events resume being attributed to
that identified person without a fresh `identify()`. Across restarts, `_load_consent_state`
(lines 290-293) re-reads the retained `IDENTIFIED_USER_KEY` and re-establishes the identified
distinct_id. Opt-out should be a clean privacy boundary.

**Fix:** In the opt-out branch, reset identity state to anonymous:

```python
else:
    _drain_and_discard()
    set_default_distinct_id(None)
    with _state_lock:
        _identified = False
        _current_distinct_id = _install_id
```

Decide explicitly whether `IDENTIFIED_USER_KEY` should be cleared on opt-out (recommended for a
clean boundary) versus retained; document the choice. Note CONSENT-06 only requires retaining
`telemetry_install_id`, not the identified user.

### WR-03: `__main__` self-test persistently mutates real consent and install-id

**File:** `desktop/telemetry.py:671-680`

**Issue:** Running `python -m desktop.telemetry` with `GENIZAH_TELEMETRY_KEY` set calls
`set_consent(True)` (line 675), which writes `telemetry_enabled=True`, mints and persists an
`install_id`, and writes consent-audit fields into the **real** `config.pkl`. It never restores
the prior consent state. A developer (or anyone) running the documented self-test seam is
silently and permanently opted into telemetry, contradicting the opt-in invariant and the
docstring promise that the self-test "Never fires in normal app startup."

**Fix:** Snapshot and restore consent around the probe, or drive the probe through an isolated
in-memory config rather than `set_consent`:

```python
prior = is_enabled()
try:
    set_consent(True)
    run_selftest()
    time.sleep(1.0)
finally:
    set_consent(prior)
```

Better: do not call the persisting `set_consent` at all — temporarily set `_enabled=True`
in-memory and emit, leaving config untouched.

### WR-04: Non-atomic identity snapshot in `_emit` — events can be mislabeled across an identify/reset race

**File:** `desktop/telemetry.py:402-409`

**Issue:** `_emit` reads `_identified` under `_state_lock` (lines 402-403), releases the lock,
builds the payload, then re-acquires `_state_lock` to read `_current_distinct_id` (lines
408-409). `_set_current_distinct_id` (lines 385-391) updates both `_current_distinct_id` and
`_identified` atomically under one lock. If `identify()` or `reset_identity()` runs on another
thread between the two reads in `_emit`, an event can be stamped with `$process_person_profile`
from one identity state and `distinct_id` from the other (e.g. identified=True paired with the
old anonymous id, or vice-versa). This is an attribution-correctness defect, not a privacy leak,
but the module is explicitly designed for multi-thread use (Qt main loop + crash hooks).

**Fix:** Take both values in a single lock acquisition:

```python
with _state_lock:
    identified = _identified
    effective_id = distinct_id or _current_distinct_id or 'system'
merged['$process_person_profile'] = identified
...
enqueue_event(event_value, scrubbed, distinct_id=effective_id)
```

### WR-05: Embedded placeholder key is truthy — events POST with a junk api_key instead of being dropped

**File:** `desktop/telemetry.py:63` (`_TELEMETRY_KEY_DEFAULT = '<embedded-placeholder>'`), `shared/posthog_server.py:228-239` and `299-309`

**Issue:** Until the real `phc_...` key is dropped in (the comment says "before Phase 114"),
`_wire_transport_config` sets the override to `'<embedded-placeholder>'`. The drain loop and
`_flush_before_exit` only skip POSTing when `not api_key` (lines 229, 299). The placeholder is a
non-empty truthy string, so once consent is granted the transport will actually `requests.post`
real (scrubbed) event payloads to PostHog with `api_key='<embedded-placeholder>'`. PostHog will
reject them, but the app is making outbound network calls carrying event data with a known-bad
key rather than dropping locally. This is a latent footgun if anyone enables telemetry before
the real key lands.

**Fix:** Treat the placeholder sentinel as "no key" so events are dropped locally until a real
key is configured — e.g. in `_wire_transport_config`, pass `None` when the resolved key equals
`_TELEMETRY_KEY_DEFAULT`, or have `_resolve_api_key` reject keys not matching `phc_`:

```python
key = os.environ.get('GENIZAH_TELEMETRY_KEY') or _TELEMETRY_KEY_DEFAULT
if key == _TELEMETRY_KEY_DEFAULT:
    key = None
set_capture_api_key(key)
```

## Info

### IN-01: `_PATH_RE` filename clause misses extensions outside 2-4 chars

**File:** `desktop/telemetry.py:142-147`

**Issue:** The bare-filename alternation `\S+\.\w{2,4}\b` only matches extensions of 2-4 word
chars. Filenames like `notes.markdown`, `data.sqlite3` (digit excluded by `\b`? — `3` is a word
char so `sqlite3`... actually 7 chars, missed), or `archive.config` are not redacted. Since the
scrubbed values in scope are app-controlled constants (`context`, `tab_name`, etc.), the practical
risk is low in Phase 111, but the defence-in-depth intent is incomplete. Consider widening to
`\.\w{1,8}\b` or relying on the banned-key drop for `filename`/`path` (already covered).

### IN-02: `_emit`/`identify` allow callers to override `$process_person_profile`

**File:** `desktop/telemetry.py:404-405` and `405` (`merged.update(props)`)

**Issue:** `_emit` sets `$process_person_profile` from the identity state at line 404, then
`merged.update(props)` (line 405) lets a caller-supplied `$process_person_profile` (it is
allowlisted, line 218) overwrite it. A `track(evt, **{'$process_person_profile': True})` call by
an anonymous user would force PostHog person-profile processing for an otherwise-anonymous
install. `identify()` legitimately needs this, but for `track()`/`track_performance()` it should
not be caller-overridable. Low impact (no producers yet), but worth locking down by re-applying
the computed value after `merged.update(props)`.

### IN-03: `track()` resolves a raw string event via `str(event)` before allowlist check — acceptable but worth a guard comment

**File:** `desktop/telemetry.py:433-438` (also `track_performance` 464-468)

**Issue:** When `event` is not a `DesktopEvent`, `event_value = str(event)` then membership in
`_VALID_EVENT_VALUES` is checked. This is correct (only enum values pass), but `str()` on an
arbitrary object could itself be a vector for surprising values; the subsequent allowlist check
neutralizes it. No defect — flagged only because the phase forbids dynamic event-name
construction (PRIV-06); a short comment that the `str()` result is immediately re-validated
against the fixed registry would make the invariant obvious to Phase 113-115 authors.

---

_Reviewed: 2026-06-14T10:02:09Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
