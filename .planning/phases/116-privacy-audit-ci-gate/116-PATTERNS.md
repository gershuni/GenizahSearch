# Phase 116: Privacy Audit + CI Gate — Pattern Map

**Mapped:** 2026-06-16
**Files analyzed:** 4
**Analogs found:** 4 / 4

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `tests/test_telemetry_priv04.py` | test | request-response (monkeypatch capture) | `tests/test_telemetry_review_fixes.py` + `tests/test_telemetry_consent_gate.py` | exact |
| `genizah_app.py` `__main__` (+30 lines) | config/entry-point | request-response (headless CLI flag) | `genizah_app.py:27486` (`--self-test-pymupdf` block) | exact |
| `docs/guides/TELEMETRY_RUNBOOK.md` | documentation | — | `docs/guides/DEPLOYMENT_TECHNICAL.md` + `docs/DOCUMENTATION_INDEX.md` | role-match |
| `.planning/REQUIREMENTS.md` INFRA-06 line | config/docs | — | The "Last updated" + "Reversed" dated note pattern already used in `.planning/REQUIREMENTS.md:176` | exact |

---

## Pattern Assignments

---

### `tests/test_telemetry_priv04.py` (test, monkeypatch-capture)

**Analogs:** `tests/test_telemetry_review_fixes.py` (primary), `tests/test_telemetry_consent_gate.py` (secondary), `tests/test_telemetry_scrubbing.py` (pure-function tests).

#### Imports block (copy verbatim from `tests/test_telemetry_review_fixes.py` lines 1-21)

```python
# -*- coding: utf-8 -*-
"""..."""

from __future__ import annotations

import queue

import pytest

import shared.posthog_server as ph
```

#### Autouse fixture (copy verbatim from `tests/test_telemetry_review_fixes.py` lines 27-56)

This is the ONLY fixture pattern used by all scrubber/consent-gate tests. Copy it exactly — do not create a new one.

```python
@pytest.fixture(autouse=True)
def _reset_telemetry_state(monkeypatch):
    """Reset desktop.telemetry + posthog_server state before/after each test."""
    fake_config: dict = {}

    def fake_load_app_config():
        return dict(fake_config)

    def fake_save_app_config(new_data: dict):
        fake_config.update(new_data)

    import genizah_core
    monkeypatch.setattr(genizah_core, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(genizah_core, 'save_app_config', fake_save_app_config)

    import desktop.telemetry as tel
    monkeypatch.setattr(tel, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(tel, 'save_app_config', fake_save_app_config)

    ph._reset_for_tests()
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)

    tel._reset_for_tests()
    tel._load_consent_state()

    yield fake_config

    tel._reset_for_tests()
    ph._reset_for_tests()
```

**Key mechanics extracted from reading the analogs:**
- `ph._event_queue` is monkeypatched to a fresh `queue.Queue(maxsize=10000)` — this is what `enqueue_event` writes to.
- `ph._event_queue.get(timeout=1.0)` captures the payload; `payload['properties']` is the already-scrubbed dict.
- `ph._event_queue.empty()` asserts zero events for the consent-gate tests.
- `tel._reset_for_tests()` + `tel._load_consent_state()` leaves consent=False (empty fake_config has no `telemetry_enabled` key).
- `tel.set_consent(True)` must be called inside each test body that needs consent enabled.

#### Core forbidden-field test pattern (from `tests/test_telemetry_review_fixes.py` lines 136-164)

The canonical shape: call `tel.track()` with a forbidden key in kwargs, then `ph._event_queue.get(timeout=1.0)` and assert the forbidden key is absent from `payload['properties']`.

```python
def test_cr01_track_with_set_nested_pii_does_not_leak():
    import desktop.telemetry as tel
    tel.set_consent(True)

    tel.track(
        tel.DesktopEvent.SELFTEST,
        **{'$set': {
            'email': 'leak@x.com',
            'p': r'C:\secret\f.pdf',
            'h': 'תשובות',
        }},
    )

    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    nested = props.get('$set', {})

    assert 'email' not in nested, "email must NOT reach enqueue_event (CR-01)"
    assert nested.get('p') == '[REDACTED]', ...
    assert nested.get('h') == '[REDACTED]', ...
```

For PRIV-04 D-01 tests, pass forbidden keys as top-level kwargs (not nested in `$set`). The scrubber operates on top-level `props` too. Permitted props that should SURVIVE (e.g., `search_mode`, `corpus_scope`) must be asserted present.

#### Pre-consent zero-emit pattern (from `tests/test_telemetry_consent_gate.py` lines 74-79)

```python
def test_no_events_before_consent():
    import desktop.telemetry as tel
    from desktop.telemetry import DesktopEvent
    tel.track(DesktopEvent.SELFTEST)
    assert ph._event_queue.empty(), "Events must not be enqueued before consent"
```

The new D-02 test extends this by calling all three entry points:

```python
# from 116-RESEARCH.md Pattern 3 / Code Examples
def test_priv04_pre_consent_zero_emit_all_entry_points():
    import desktop.telemetry as tel
    assert not tel.is_enabled()
    tel.track(tel.DesktopEvent.SELFTEST)
    tel.track_performance(tel.DesktopEvent.SESSION_PERF, duration_ms=100.0)
    tel.track_error('ctx', ValueError('test'))
    assert ph._event_queue.empty(), (
        "No event must be enqueued before consent — all three entry points "
        "must respect is_enabled() gate (CONSENT-01 / D-02)"
    )
```

#### Pure-function test pattern (from `tests/test_telemetry_scrubbing.py` lines 1-34)

For tests that only call `_scrub_props` or `_scrub_value` directly (no `track()`, no queue):

```python
# No autouse fixture needed for pure-function tests, but the autouse fixture
# from the new file's scope still applies — that is fine.
from desktop.telemetry import _scrub_props

def test_banned_keys_stripped():
    result = _scrub_props({
        'query': 'some search text',
        'filename': 'a.pdf',
        'path': '/home/user/data',
        'platform': 'desktop',
    })
    assert 'query' not in result
    assert 'filename' not in result
    assert 'path' not in result
    assert result.get('platform') == 'desktop'
```

**Note:** `tests/test_telemetry_scrubbing.py` already covers `query`/`filename`/`path` as banned keys (line 22-33) and Hebrew redaction (line 104-112). The new PRIV-04 tests must push these through the FULL `track()` pipeline to assert the payload reaching `enqueue_event` is clean — this is the distinct value over the pure-function tests.

---

### `genizah_app.py` `__main__` — `--telemetry-selftest` block (entry-point, headless CLI)

**Analog:** `genizah_app.py:27485-27530` (`--self-test-pymupdf` block).

#### Template — the `--self-test-pymupdf` block (lines 27485-27530)

```python
if __name__ == "__main__":
    # Phase 95 HIGH-5 review fix — PyInstaller packaging self-test.
    # MUST be checked BEFORE QApplication construction so the EXE runs
    # headlessly (no Qt event loop, no GUI side effects).
    if "--self-test-pymupdf" in sys.argv:
        import pathlib as _pathlib
        try:
            import fitz  # PyMuPDF — D-43 packaging dependency
        except Exception as _e:
            print(f"PYMUPDF_FAIL: import failed: {_e}", file=sys.stderr)
            sys.exit(1)
        # ... test logic ...
        try:
            # ... open fixture, extract text ...
            print("PYMUPDF_OK")
            sys.exit(0)
        except Exception as _e:
            print(f"PYMUPDF_FAIL: extraction raised: {_e!r}", file=sys.stderr)
            sys.exit(1)
    # --- QApplication construction follows AFTER all headless blocks ---
    try:
        import ctypes
        ...
    app = QApplication(sys.argv)
```

**Placement rule (from reading lines 27485-27540):** New headless blocks go BEFORE the `try: import ctypes` block and BEFORE `app = QApplication(sys.argv)` at line 27540. The `--telemetry-selftest` block should be inserted BEFORE `--self-test-pymupdf` (or immediately after it) — either position is fine as long as it precedes `QApplication`.

#### The `__main__` self-test probe pattern (`desktop/telemetry.py:1736-1748`)

This is the authoritative in-memory consent toggle pattern. The new `genizah_app.py` block MUST replicate it exactly:

```python
# desktop/telemetry.py:1736-1748 — the WR-03 pattern
prior_enabled = is_enabled()
try:
    with _enabled_lock:
        _enabled = True  # in-memory only, no config.pkl write
    _wire_transport_config()  # apply the env key for this one run
    run_selftest()
    import time
    time.sleep(1.0)
    print('telemetry: self-test complete (check PostHog for desktop_selftest event)')
finally:
    with _enabled_lock:
        _enabled = prior_enabled
```

**Critical constraint (Pitfall 2):** NEVER call `tel.set_consent(True)` from the selftest block — it writes to `config.pkl`. Use `with _tel._enabled_lock: _tel._enabled = True` in-memory only, restored in `finally`.

#### `run_selftest()` signature (`desktop/telemetry.py:836-848`)

```python
def run_selftest() -> None:
    """Dev-only pipeline probe — emits one desktop_selftest event.

    Consent-gated. Reachable only via explicit invocation (--telemetry-selftest
    CLI flag wired in genizah_app in a later phase, or via python -m desktop.telemetry).
    Never fires in normal app startup. Never raises.
    """
    try:
        if not is_enabled():
            return
        track(DesktopEvent.SELFTEST)
    except Exception:
        logger.debug('telemetry: run_selftest() silently failed', exc_info=True)
```

**Drop counter:** `shared/posthog_server.get_dropped_event_count()` (`:88`) — increment means no key or transport exception. After `run_selftest()` + `time.sleep(1.5)`, check this counter. Zero = `SSL_OK`; nonzero = `SSL_FAIL`.

#### Complete `--telemetry-selftest` block to insert (from RESEARCH.md Code Examples)

```python
    # Phase 116 — telemetry pipeline + SSL self-test (D-04/D-05).
    # MUST be checked BEFORE QApplication construction (headless, no Qt event loop).
    if "--telemetry-selftest" in sys.argv:
        import desktop.telemetry as _tel
        import shared.posthog_server as _ph
        import time as _time

        _offline = "--telemetry-selftest-offline" in sys.argv
        _prior_enabled = _tel.is_enabled()
        try:
            with _tel._enabled_lock:
                _tel._enabled = True   # in-memory only — no config.pkl write (WR-03)
            _tel._wire_transport_config()
            if _offline:
                # Offline arm: verify chain runs without crash/dialog; skip network POST
                _tel.run_selftest()
                # No sleep — just verify enqueue succeeded without blocking
                print("OFFLINE_OK")
                sys.exit(0)
            # Online arm: emit + wait for drain thread POST attempt
            _tel.run_selftest()
            _time.sleep(1.5)
            _dropped = _ph.get_dropped_event_count()
            if _dropped > 0:
                print(
                    f"SSL_FAIL: {_dropped} events dropped (no key, SSL error, or no network)",
                    file=sys.stderr,
                )
                sys.exit(1)
            print("SSL_OK")
            sys.exit(0)
        except Exception as _e:
            print(f"SSL_FAIL: {_e!r}", file=sys.stderr)
            sys.exit(1)
        finally:
            with _tel._enabled_lock:
                _tel._enabled = _prior_enabled
```

---

### `docs/guides/TELEMETRY_RUNBOOK.md` (documentation)

**Analog:** `docs/guides/DEPLOYMENT_TECHNICAL.md` (heading conventions, `> Last updated:` format, `---` section separators) + `docs/DOCUMENTATION_INDEX.md` (how guide entries are listed).

#### Heading/structure conventions (from `docs/guides/DEPLOYMENT_TECHNICAL.md` lines 1-8)

```markdown
# GenizahSearch Technical Deployment Guide

> Last updated: 2026-03-13
> For: Developers, System Administrators, AI Assistants

---

## Architecture Overview (February 2026)
```

New file should open with:

```markdown
# GenizahSearch Desktop Telemetry Runbook

> Last updated: 2026-06-16
> For: Developers, Release Engineers

---
```

Then five `##` sections (D-08 content: shared-project+namespace, embedded key+rotation+knobs, two drop counters, `--telemetry-selftest` usage, opt-out behavior).

#### DOCUMENTATION_INDEX.md — how a guide entry is listed (lines 75-79)

```markdown
### For Developers
- **[DEVELOPER_GUIDE.md](guides/DEVELOPER_GUIDE.md)** - Getting started with local development
- **[DEPLOYMENT_TECHNICAL.md](guides/DEPLOYMENT_TECHNICAL.md)** - Technical deployment and configuration
- **[SUPABASE_GUIDE.md](guides/SUPABASE_GUIDE.md)** - Working with the Supabase database
- **[MULTITENANT.md](guides/MULTITENANT.md)** - v7.12 Path B multitenant architecture reference ...
```

New entry to add under "For Developers":

```markdown
- **[TELEMETRY_RUNBOOK.md](guides/TELEMETRY_RUNBOOK.md)** - Desktop telemetry operational guide (shared PostHog project, key rotation, drop counters, self-test flag, opt-out behavior)
```

Also update `> Last updated: 2026-03-26` → `2026-06-16` in DOCUMENTATION_INDEX.md (Pitfall 6 from RESEARCH.md).

---

### `.planning/REQUIREMENTS.md` INFRA-06 — stale wording amendment (config/docs)

**Analog:** The existing dated inline note pattern at `.planning/REQUIREMENTS.md:176`:

```
*Last updated: 2026-06-14 — REVISED during Phase 111 discussion: reversed to ONE shared web PostHog project + web-aligned identity (new IDENT category, 4 reqs → 40 total); CONSENT-05 / USAGE-05 / INFRA-01 amended; Out-of-Scope updated. See `research/POSTHOG-PROJECT-DECISION.md`.*
```

And the "A separate desktop PostHog project" Out-of-Scope row at line 117 which uses "Reversed 2026-06-14" inline:

```
| A separate desktop PostHog project | Reversed 2026-06-14 — desktop uses the shared web project ...
```

#### Current INFRA-06 line (line 80) — what the executor READS before editing

```
- [ ] **INFRA-06**: Operational runbook — the desktop PostHog project is **isolated** from the web project; the embedded ingest key is documented as write-only (treated as abuse-tolerant with a rotation procedure, not a secret); and both `get_dropped_event_count()` drop counters (`web.api_hardening` + `shared.posthog_server`) are monitored after launch.
```

#### Target — amended form

Replace the `[ ]` with `[x]` and append a dated parenthetical note matching the inline-note style already used in this file:

```
- [x] **INFRA-06**: Operational runbook — the shared PostHog project (id 134161, EU) separates desktop events by `platform=desktop` + `desktop_` event-name namespace (NOT an isolated project — see note); the embedded ingest key is documented as write-only (treated as abuse-tolerant with a rotation procedure, not a secret); and both `get_dropped_event_count()` drop counters (`web.api_hardening` + `shared.posthog_server`) are monitored after launch. *(AMENDED 2026-06-16: "isolated project" wording was stale since the 2026-06-14 reversal — see `.planning/research/POSTHOG-PROJECT-DECISION.md`.)*
```

And update the status table row (line ~166) from `Pending` to `Complete`:

```
| INFRA-06 | Phase 116 | Complete |
```

---

## Shared Patterns

### Telemetry autouse fixture
**Source:** `tests/test_telemetry_review_fixes.py` lines 27-56 (identical copy also in `tests/test_telemetry_consent_gate.py` lines 24-59).
**Apply to:** `tests/test_telemetry_priv04.py` — copy verbatim, no modification.
**Effect:** Provides isolated consent state + fresh queue per test; `yield fake_config` lets tests inspect what was "saved to config.pkl".

### Pre-Qt headless block pattern
**Source:** `genizah_app.py:27485-27530` (`--self-test-pymupdf`).
**Apply to:** `--telemetry-selftest` block in `genizah_app.py __main__`.
**Key rules:**
- All imports inside the `if "..." in sys.argv:` block (prefixed `_` to avoid polluting namespace).
- `try/except Exception` wraps all logic; failure prints `*_FAIL` to stderr + `sys.exit(1)`.
- Success prints `*_OK` to stdout + `sys.exit(0)`.
- Runs BEFORE `app = QApplication(sys.argv)`.

### In-memory consent toggle (no config.pkl write)
**Source:** `desktop/telemetry.py:1736-1748` (`__main__` self-test probe).
**Apply to:** `--telemetry-selftest` block in `genizah_app.py`.
**Key:** `with _tel._enabled_lock: _tel._enabled = True` + `finally: _tel._enabled = _prior_enabled`. Never `set_consent(True)`.

### Docs guide heading convention
**Source:** `docs/guides/DEPLOYMENT_TECHNICAL.md` lines 1-8.
**Apply to:** `docs/guides/TELEMETRY_RUNBOOK.md`.
**Format:** `# Title\n\n> Last updated: YYYY-MM-DD\n> For: audience\n\n---\n\n## Section`.

### DOCUMENTATION_INDEX.md entry format
**Source:** `docs/DOCUMENTATION_INDEX.md` lines 75-79 ("For Developers" bullet list).
**Apply to:** Adding TELEMETRY_RUNBOOK.md entry + bumping "Last updated" timestamp.

### REQUIREMENTS.md inline amendment note
**Source:** `.planning/REQUIREMENTS.md:176` (bottom footer line + line 117 "Reversed 2026-06-14" inline parenthetical).
**Apply to:** INFRA-06 line amendment.
**Format:** Append `*(AMENDED YYYY-MM-DD: explanation — see reference.)*` at the end of the amended line.

---

## No Analog Found

None. All four files have close existing analogs within the codebase.

---

## Metadata

**Analog search scope:** `tests/`, `genizah_app.py` `__main__`, `desktop/telemetry.py`, `docs/guides/`, `.planning/REQUIREMENTS.md`
**Files scanned:** `tests/test_telemetry_review_fixes.py`, `tests/test_telemetry_scrubbing.py`, `tests/test_telemetry_consent_gate.py`, `genizah_app.py:27469-27548`, `desktop/telemetry.py` (targeted ranges), `docs/DOCUMENTATION_INDEX.md`, `docs/guides/DEPLOYMENT_TECHNICAL.md`, `.planning/REQUIREMENTS.md`
**Pattern extraction date:** 2026-06-16
