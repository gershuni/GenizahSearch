---
phase: 116-privacy-audit-ci-gate
reviewed: 2026-06-16T11:35:00Z
depth: standard
files_reviewed: 5
files_reviewed_list:
  - desktop/telemetry.py
  - shared/posthog_server.py
  - genizah_app.py
  - tests/test_telemetry_priv04.py
  - tests/test_telemetry_selftest.py
findings:
  critical: 0
  warning: 2
  info: 3
  total: 5
status: resolved
resolution: "WR-01 + WR-02 fixed in commit aa9f8d3b (2026-06-16). 0 Critical; both Warnings addressed; Info items accepted as benign. Full telemetry/guard regression green (238 passed)."
---

# Phase 116: Code Review Report

**Reviewed:** 2026-06-16T11:35:00Z
**Depth:** standard (per-file analysis with language-specific checks)
**Files Reviewed:** 5 (Phase-116 diff only; `genizah_app.py` limited to the new `--telemetry-selftest` block)
**Status:** resolved (both Warnings fixed in commit `aa9f8d3b`, 2026-06-16)

> **Resolution (2026-06-16):** WR-01 (test_telemetry_priv04.py daemon/network leak to
> production PostHog + `.get()` race) and WR-02 (test_telemetry_selftest.py truncated
> static-block slice missing the `finally` clause) were both fixed in commit `aa9f8d3b`.
> The 3 Info items were reviewed and accepted as benign (no live callsite / local-only /
> harmless). Full telemetry + guard regression suite green (238 passed) after the fixes.

## Summary

The core privacy invariant of this phase **holds**. I traced every reviewed path adversarially,
including the real (non-mocked) network-POST path, and confirmed:

- `_safe_context` correctly collapses filename-shaped contexts (`manuscript_notes.docx`,
  `report.pdf`, `config.json`, uppercase `MyFile.PDF`) to `'unregistered'` while preserving
  legitimate dotted code labels (`search_tab.run`, `app.crash`, `search_tab.run_query`,
  `tab.open`). `_CONTEXT_RE` rejects leading/trailing/double dots, so `rsplit('.', 1)[1]` can
  never index-error or return an empty segment. Case-folding via `.lower()` is correct. Hebrew /
  space-bearing / slash-bearing contexts already fail `_CONTEXT_RE` and collapse — no leak path.
- `send_selftest_event_sync()` satisfies every clause of its contract: returns `'NO_KEY'` with
  **zero** network calls when unkeyed (verified by instrumenting `requests.post`), performs
  **exactly one** `requests.post`, returns `'SSL_OK'` only on HTTP 2xx, returns `'SSL_FAIL ...'`
  on any exception or non-2xx, never raises, and never touches `_event_queue` / `_dropped_events`
  / the daemon. The payload is PII-free (`platform=desktop`, `$process_person_profile=False`,
  `distinct_id='system'`, hardcoded event name).
- The `--telemetry-selftest` block runs before `QApplication` (line 27489 < 27584), toggles
  `_enabled` in-memory under `_enabled_lock`, **never** calls `set_consent`, restores
  `_prior_enabled` in `finally`, and drives the success signal off the sync helper's return (not
  the drop counter). `sys.exit()` raises `SystemExit` (a `BaseException`, not `Exception`), so it
  correctly bypasses the `except Exception` handler and still runs the `finally`. The offline arm
  makes no network call and exits fast.

No Critical findings. Two Warnings concern **test robustness/hygiene** (not privacy leaks): the
new PRIV-04 tests inherit a latent daemon-race + real-production-network-POST pattern from their
analog file, and the self-test static guard under-covers the block it inspects. Three Info items
note a theoretical extension-set false-positive and minor documentation/consistency nits.

## Warnings

### WR-01: PRIV-04 tests make real network POSTs to production PostHog and carry a latent daemon-consume race

**File:** `tests/test_telemetry_priv04.py:79-341` (every test that calls `set_consent(True)` then `track(...)` then `_event_queue.get(timeout=1.0)`)

**Issue:** The autouse fixture monkeypatches a fresh `ph._event_queue` but `ph._reset_for_tests()`
explicitly does **not** stop the shared drain daemon, and `_drain_thread_started` is never reset.
Meanwhile `tel.set_consent(True)` calls `_wire_transport_config()`, which resolves the **real
embedded `_TELEMETRY_KEY_DEFAULT`** (`'phc_CGTsV72F...'`, a live ingestion key for production
project 134161 — NOT the unfilled sentinel). The subsequent `tel.track(...)` calls
`_start_drain_thread_once()`. I confirmed empirically that the daemon then drains the monkeypatched
queue and **attempts a real `requests.post` to `https://eu.i.posthog.com/capture`** (instrumented:
`!!! REAL NETWORK POST ATTEMPTED to https://eu.i.posthog.com/capture`).

Two consequences:
1. **Latent flake:** the daemon's `_event_queue.get(timeout=60)` races the test's
   `.get(timeout=1.0)`. The main thread usually wins (tests pass in ~0.25s), but I reproduced a
   forced loss — when the daemon reads first, the test's `.get()` raises `queue.Empty` and the
   test fails. The daemon persists across the whole session, so every PRIV-04 test after the first
   one races a live daemon. Under CI load / a slow Windows runner this can intermittently fail.
2. **Network side-effect in a "lightweight unit" suite:** each run emits real (scrubbed) `desktop_*`
   events to production analytics and makes the suite depend on network reachability — contrary to
   the D-01 "fast pure-function" intent and the analog file's own header claim ("NO real network
   calls made").

This pattern is **pre-existing** (copied verbatim from `test_telemetry_review_fixes.py` per the
plan), and the privacy invariant still holds (I verified the on-wire payload is clean: `path`
dropped, Hebrew `context` → `'unregistered'`). So this is a test-hygiene/determinism defect, not a
privacy leak — but the new file multiplies the surface by 8 additional `track()`-after-consent
tests.

**Fix:** Neutralize the daemon + key in the autouse fixture so the capture is deterministic and
offline. Add to `_reset_telemetry_state` (before `yield`):

```python
# Prevent the shared drain daemon from racing the test .get() and from
# making a real network POST with the embedded production key.
monkeypatch.setattr(ph, '_resolve_api_key', lambda: '')   # daemon drains -> 'continue', no POST
# (belt-and-braces) hard-fail if any test path reaches the network:
monkeypatch.setattr(ph.requests, 'post',
                    lambda *a, **k: (_ for _ in ()).throw(AssertionError('no network in unit tests')))
```

Setting `_resolve_api_key` to return `''` makes the daemon `continue` past the POST (and the
helper-under-test isn't exercised here, so it's unaffected), while the `requests.post` guard turns
any accidental network attempt into a loud failure instead of a silent production emit. The test's
own `.get(timeout=1.0)` still captures the payload because the daemon no longer POSTs — but the
race on `get()` remains, so ALSO drain deterministically or assert via a captured copy. The
cleanest fix is to monkeypatch `enqueue_event` to append payloads to a list (no queue, no daemon),
which removes the race entirely; if you keep the queue-capture pattern, the two monkeypatches above
at minimum stop the production POSTs.

### WR-02: `--telemetry-selftest` static guard slices only the first 1600 chars, missing the `finally` block it most needs to cover

**File:** `tests/test_telemetry_selftest.py:163-176` (`test_block_uses_in_memory_consent_toggle_not_set_consent` and `test_signal_driven_by_sync_helper_not_drop_counter`)

**Issue:** Both guards do `block = src[guard:guard + 1600]` and then assert `'set_consent(' not in
block` / `'get_dropped_event_count' not in block`. The actual block is **1911 chars** long (verified),
so the last ~311 chars — which is exactly the `finally:` restore clause — are **not** inspected. The
`finally` is the single most likely place a future maintainer would (wrongly) add cleanup that
persists consent or keys off the drop counter, and the guard would silently miss it. The assertions
pass today only because the current `finally` is innocuous.

**Fix:** Slice to the real end of the block instead of a magic 1600 byte window:

```python
guard = src.index('if "--telemetry-selftest" in sys.argv')
end = src.index('# Phase 95 HIGH-5', guard)   # next dedented __main__ block
block = src[guard:end]
```

Or widen the window (`guard + 2200`) so it provably covers the whole block including `finally`.

## Info

### IN-01: `_CONTEXT_FILE_EXTENSIONS` will over-collapse a legitimate code label whose final segment equals a short extension token

**File:** `desktop/telemetry.py:355-363, 384`

**Issue:** The curated set contains short tokens (`db`, `dat`, `md`, `tar`, `exe`, `ico`, `one`?
no — but `db`/`md`/`dat` are real risks) that are plausible final segments of legitimate dotted
code labels, e.g. a context like `cache.db`, `export.md`, or `session.dat` would be collapsed to
`'unregistered'`. This is a **false-positive** (over-redaction), the privacy-safe direction. I
grepped all live `track_error` / `context=` callsites: current labels (`search_tab.run`,
`app.crash`, etc.) use method-ish segments and none collide, so there is **no current regression** —
this is a future-proofing caveat only. Conversely, filenames with extensions NOT in the curated set
(`notes.markdown`, `scan.jp2`, `book.azw3`, `file.log`, `archive.tgz`, `x.docm`) survive verbatim;
acceptable because (a) `context` callsites are hardcoded static labels enforced by the D-17
dynamic-string guard, and (b) the curated set covers every extension a My-Library/document producer
could realistically pass.

**Fix:** No change required. Optionally add a one-line comment at the callsite noting that a code
label whose final segment is a real extension token (`x.db`, `x.md`) will be collapsed, so authors
pick method-ish final segments.

### IN-02: `send_selftest_event_sync` `'SSL_FAIL {exc!r}'` may embed environment detail; benign for a release-engineer tool but worth noting

**File:** `shared/posthog_server.py:454-455`

**Issue:** On exception the helper returns `f'SSL_FAIL {exc!r}'`, and the CLI block prints that token
to stderr. A `requests` exception repr can contain the target URL / proxy / local cert path. This is
a **developer/release-engineer self-test** (run intentionally with `--telemetry-selftest`), not a
telemetry payload that reaches the network — so it is not a privacy leak (the exception text is
never POSTed; it is only printed locally). Noted only because the surrounding phase is privacy-
sensitive: the failure token deliberately surfaces what the daemon's bare-except swallows, which is
the intended REVIEWS HIGH #1 behavior.

**Fix:** None required. If you want stderr to be minimal, `f'SSL_FAIL {type(exc).__name__}'` conveys
the failure class without any path/URL detail.

### IN-03: Self-test leaves the embedded key wired into the shared transport on exit (not restored in `finally`)

**File:** `genizah_app.py:27499-27521` (the `try`/`finally` restores `_enabled` but not the transport key/host/scrub-hook set by `_wire_transport_config()`)

**Issue:** The `finally` restores `_tel._enabled = _prior_enabled` but does not undo
`_wire_transport_config()` (which called `set_capture_api_key(...)` / `register_scrub_hook(...)`).
This is **harmless** in practice because the block always `sys.exit()`s — the process terminates
immediately, so the wired key never outlives the probe. Flagged only for completeness: if this block
were ever refactored to fall through into normal startup instead of exiting, the transport would be
left keyed even when `_prior_enabled` was False.

**Fix:** No change required while the block always exits. If defensiveness is wanted, the `finally`
could also clear the transport when `not _prior_enabled` (`_ph.set_capture_api_key(None)` /
`_ph.register_scrub_hook(None)`), but this is optional given the guaranteed `sys.exit`.

---

_Reviewed: 2026-06-16T11:35:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
