# Phase 116: Privacy Audit + CI Gate — Research

**Researched:** 2026-06-16
**Domain:** Telemetry validation, PyInstaller SSL bundling, test extension, operational runbook
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01** PRIV-04 tests: scrubber-unit level only — push representative forbidden inputs through `track()` / `track_error()` / `_scrub_props` and assert no forbidden field in the `enqueue_event` payload. Do NOT build a Qt producer-path harness.
- **D-02** Pre-consent zero-emit (CONSENT-01): add a fresh-`config.pkl` (consent unset/false) test asserting zero events reach the queue.
- **D-03** Exact fixture/scenario selection: Claude's discretion. Cover SC#2 crash-traceback / My-Library search / composition cases at the input level; reuse existing fixtures.
- **D-04** `--telemetry-selftest` CLI flag: pre-Qt headless flag in `genizah_app.py`, modeled on `--self-test-pymupdf` (~:27486). Reuses `desktop/telemetry.run_selftest()`. Prints machine-readable token + non-zero exit on failure. Key required.
- **D-05** Offline arm: same flag + network disabled — must return fast (bounded by `requests.post(timeout=2.0)`), no crash/dialog/delay.
- **D-06** Clean-machine run = HUMAN-UAT on a Windows VM with NO Python installed. Closes Phase 114's open live-delivery UAT.
- **D-07** INFRA-06 stale wording: amend REQUIREMENTS.md INFRA-06 from "isolated" to "shared" with dated note pointing at POSTHOG-PROJECT-DECISION.md.
- **D-08** Runbook at `docs/guides/TELEMETRY_RUNBOOK.md` with 5 content sections: (a) shared project + namespace separation, (b) publishable embedded key + rotation + env knobs, (c) two drop counters, (d) `--telemetry-selftest` usage, (e) opt-out behavior.
- **D-09** CI: keep PRIV-04 tests in existing `tests` job (`pytest tests/ -m "not gui"`); both ubuntu-latest and windows-latest; NO new CI job.
- **D-10** Milestone-exit regression gate: document ~290 telemetry tests as a required-green gate in VERIFICATION.md + runbook.

### Claude's Discretion
- PRIV-04 fixture/scenario selection (light, reuse existing patterns).
- `--telemetry-selftest` exact flag name, output tokens, exit codes, offline-arm mechanism.
- Runbook section ordering and wording.

### Deferred Ideas (OUT OF SCOPE)
- WEB-F1: web `search_executed` query-text cleanup.
- ERR-01: handled/non-fatal error counting.
- CONSENT-F1: "reset telemetry id" affordance.
- CRASH-F1: "send logs" native-crash upload.
- FLAG-F1: PostHog feature flags / remote config.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PRIV-03 | Static AST CI guard (`test_telemetry_no_direct_posthog.py`) on both Ubuntu + Windows CI | Already delivered in Phase 111-03; SC#1 satisfied by existing `tests` job; research confirms guard is green and needs no re-implementation. |
| PRIV-04 | Automated tests that representative crash / search / My-Library scenarios never produce a payload with a forbidden field, and that nothing fires before consent | Research confirms the fixture/monkeypatch pattern (D-01/D-02); identifies exact scrubber surface + existing test infrastructure to extend. |
| INFRA-06 | Operational runbook documenting the shared PostHog project, embedded key posture, two drop counters, and self-test flag | Research confirms both drop counters, key-resolution order, runbook content. Stale "isolated" wording in REQUIREMENTS.md must be amended (D-07). |
</phase_requirements>

---

## Summary

Phase 116 is the final v8.1.0 milestone close: zero new producers, zero new chokepoint machinery. It verifies and documents the telemetry stack built in Phases 111-115.

**What this phase actually produces:**
1. `tests/test_telemetry_priv04.py` — ~10-15 new scrubber-unit tests extending the existing fixture pattern; covers 4 representative forbidden-input scenarios + pre-consent zero-emit.
2. A `--telemetry-selftest` block (~30 lines) in `genizah_app.py` `__main__`, modeled exactly on the existing `--self-test-pymupdf` block at line 27486.
3. `docs/guides/TELEMETRY_RUNBOOK.md` — new operational guide, ~200-300 lines.
4. `docs/DOCUMENTATION_INDEX.md` update — add runbook entry.
5. `.planning/REQUIREMENTS.md` edit — amend INFRA-06 "isolated project" wording (stale since 2026-06-14 reversal).
6. `VERIFICATION.md` note — document the ~290-test telemetry regression suite as a milestone-exit gate.
7. One HUMAN-UAT item: clean Windows VM run of `GenizahSearchPro.exe --telemetry-selftest`.

**Primary recommendation:** Plan in three focused waves: (W1) PRIV-04 tests + pre-consent assertion; (W2) `--telemetry-selftest` CLI flag; (W3) runbook + REQUIREMENTS.md amendment + VERIFICATION.md gate note. No code architectural risk.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Privacy scrubber tests (PRIV-04) | Test layer (pytest, pure-function) | — | `_scrub_props` is a pure dict→dict function; no Qt needed; monkeypatch captures `enqueue_event` payloads |
| Pre-consent zero-emit assertion | Test layer | — | `set_consent()` / `_reset_for_tests()` pattern already established in existing consent-gate tests |
| `--telemetry-selftest` CLI flag | Desktop app entry point (`genizah_app.py __main__`) | Transport (`shared/posthog_server.py`) | Pre-Qt headless, before `QApplication`; calls `run_selftest()` → `track()` → queue → drain thread → `requests.post(timeout=2.0)` |
| SSL/certifi bundling | PyInstaller build artifact (`_internal/certifi/cacert.pem`) | `hook-certifi.py` auto-hook | `cacert.pem` is auto-bundled via `_pyinstaller_hooks_contrib`; no spec change needed |
| Operational runbook | Documentation layer (`docs/guides/`) | `docs/DOCUMENTATION_INDEX.md` | New file; references real code surface |
| CI gate verification | Existing `.github/workflows/ci.yml` `tests` job | — | SC#1 already satisfied; no YAML changes needed |

---

## Standard Stack

### No new packages needed

This phase installs zero new dependencies. [VERIFIED: inspected `requirements-lock.txt` and `GenizahSearchPro.spec`]

| Component | Location | Purpose | Status |
|-----------|----------|---------|--------|
| `requests` 2.32.5 | `requirements-lock.txt` | Transport for `posthog_server.py` | Already in use |
| `certifi` 2026.2.25 | `requirements-lock.txt` | CA bundle for HTTPS POST to PostHog EU | Already bundled in frozen exe |
| `pyinstaller-hooks-contrib` 2026.3 | dev dep | Auto-bundles `certifi/cacert.pem` via `hook-certifi.py` | Already active |
| `pytest` | CI only | Test runner | Already installed |

## Package Legitimacy Audit

> No new packages are installed in this phase. This section is intentionally minimal.

**Packages removed due to slopcheck [SLOP] verdict:** none — no new packages.
**Packages flagged as suspicious [SUS]:** none.

---

## Architecture Patterns

### System Architecture Diagram

```
tests/test_telemetry_priv04.py
  └── monkeypatch ph._event_queue (fresh queue)
  └── tel._reset_for_tests() + tel._load_consent_state()
       │
       ├── PRIV-04 forbidden-input path:
       │   tel.set_consent(True)
       │   tel.track(DesktopEvent.CRASH, **{forbidden_input_prop})
       │                                     │
       │                          desktop/telemetry.py::_emit()
       │                              → _scrub_props(props)   [PRIV-01]
       │                              → _validate_props(props) [PRIV-02]
       │                              → ph.enqueue_event()
       │                                        │
       │                              ph._event_queue.get()  ← assert payload
       │
       └── Pre-consent path:
           tel._load_consent_state()  (config.pkl absent → consent = False)
           tel.track(DesktopEvent.SELFTEST)
           → is_enabled() returns False → early return
           → ph._event_queue remains EMPTY  ← assert

genizah_app.py __main__ (--telemetry-selftest block, pre-Qt)
  ├── set_consent_in_memory(True)   [no config.pkl write]
  ├── _wire_transport_config()      [reads GENIZAH_TELEMETRY_KEY]
  ├── run_selftest()                [emits desktop_selftest event]
  ├── time.sleep(1.0)               [allows daemon drain thread to POST]
  ├── print("SSL_OK")
  └── sys.exit(0)   [or SSL_FAIL + exit(1) on transport exception]

Offline arm (same flag, no network):
  requests.post(timeout=2.0) raises ConnectionError/SSLError
  → silently caught in _drain_posthog_queue exception handler
  → print("OFFLINE_OK: no crash, no dialog")
  → sys.exit(0)
```

### Recommended Project Structure

No structural changes. Adds:

```
tests/
└── test_telemetry_priv04.py    # new — PRIV-04 forbidden-field + pre-consent tests

docs/guides/
└── TELEMETRY_RUNBOOK.md        # new operational guide

genizah_app.py                  # ~30 lines added in __main__ (--telemetry-selftest)
docs/DOCUMENTATION_INDEX.md    # 1-line entry added
.planning/REQUIREMENTS.md      # INFRA-06 wording amended (1 line + dated note)
```

### Pattern 1: Existing scrubber-unit test fixture (EXTEND, not rewrite)

The established pattern from `tests/test_telemetry_review_fixes.py` and `test_telemetry_consent_gate.py`:

```python
# Source: tests/test_telemetry_review_fixes.py (autouse fixture)
@pytest.fixture(autouse=True)
def _reset_telemetry_state(monkeypatch):
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

**Key insight:** `ph._event_queue.get(timeout=1.0)` captures the payload that went through `enqueue_event`. `payload['properties']` is the scrubbed, allowlisted dict. Assert `'query' not in payload['properties']`, `'filename' not in`, etc.

### Pattern 2: Pre-Qt headless CLI flag (--self-test-pymupdf template)

```python
# Source: genizah_app.py:27489 — exact model for --telemetry-selftest
if "--self-test-pymupdf" in sys.argv:
    try:
        import fitz
    except Exception as _e:
        print(f"PYMUPDF_FAIL: import failed: {_e}", file=sys.stderr)
        sys.exit(1)
    # ... test logic ...
    print("PYMUPDF_OK")
    sys.exit(0)
```

The `--telemetry-selftest` block must be placed **before** the `--self-test-pymupdf` block (or right after it), before `QApplication` construction. It is idiomatic to print a machine-readable token (`SSL_OK` / `SSL_FAIL`) and exit non-zero on failure. Exact pattern for the selftest:

```python
# Proposed (D-04/D-05) — ~30 lines in genizah_app.py __main__
if "--telemetry-selftest" in sys.argv:
    import desktop.telemetry as _tel
    import shared.posthog_server as _ph
    import time as _time

    # Temporarily enable consent in-memory (no config.pkl write — mirrors __main__ probe)
    _offline = "--telemetry-selftest-offline" in sys.argv
    _prior = _tel.is_enabled()
    try:
        with _tel._enabled_lock:
            _tel._enabled = True
        _tel._wire_transport_config()
        if _offline:
            # Offline: just assert the app doesn't crash without network
            print("OFFLINE_OK: telemetry degrades silently (not verified here — run with network disabled)")
            sys.exit(0)
        _tel.run_selftest()
        _time.sleep(1.5)   # allow daemon drain thread one POST attempt
        dropped = _ph.get_dropped_event_count()
        if dropped > 0:
            print(f"SSL_FAIL: {dropped} events dropped (likely no key or SSL error)", file=sys.stderr)
            sys.exit(1)
        print("SSL_OK")
        sys.exit(0)
    except Exception as _e:
        print(f"SSL_FAIL: {_e!r}", file=sys.stderr)
        sys.exit(1)
    finally:
        with _tel._enabled_lock:
            _tel._enabled = _prior
```

**Note on offline arm:** The true offline test is: invoke `--telemetry-selftest` on a machine with the network cable pulled / firewall rule blocking `eu.i.posthog.com`. The `requests.post(timeout=2.0)` will raise a `ConnectionError`/`SSLError`, which is silently swallowed by `_drain_posthog_queue`'s bare `except Exception: pass`. The app must start, emit the selftest event, and terminate within ~3 seconds (2.0s transport timeout + 1.5s sleep). This is validated by the clean-VM run (D-06), not by automated test.

**Alternative for offline arm mechanism (D-05 discretion):** Use `--telemetry-selftest-offline` as a second flag (simpler than an env toggle) that skips the actual POST and just verifies the consent gate + scrubber chain returns without error. This is the cleaner approach.

### Pattern 3: PRIV-04 representative forbidden inputs

The four SC#2-named scenarios mapped to concrete test inputs:

| Scenario | Forbidden Input Shape | Forbidden Field Verified Absent |
|----------|-----------------------|--------------------------------|
| My-Library search path | `{'path': r'C:\Users\gersh\Library\teshuvot.pdf', 'context': 'search'}` | `path`, plus path value not in any allowed prop |
| My-Library filename leak | `{'filename': 'manuscript_notes.docx', 'search_mode': 'keyword'}` | `filename` |
| Hebrew query string | `{'query': 'תשובות הרמב״ם', 'corpus_scope': 'local'}` | `query`, plus Hebrew value on any allowed prop |
| Crash traceback with frame locals | `{'frame_locals': {'query': 'some text'}, 'exc_type': 'ValueError', 'traceback_raw': '...'}` | `frame_locals`, `traceback_raw` |
| Hostname / username leak | `{'hostname': 'hillelpc', 'username': 'gersh', 'app_version': '8.1.0'}` | `hostname`, `username` |

For PRIV-04's pre-consent assertion (D-02): call `tel.track()`, `tel.track_performance()`, `tel.track_error()` with the autouse fixture in fresh-config (consent unset) state and assert `ph._event_queue.empty()`. The existing `test_no_events_before_consent` in `test_telemetry_consent_gate.py` (line 74) covers a single `track()` call — the new test should cover all three public entry points.

### Anti-Patterns to Avoid

- **Re-implementing PRIV-03:** The AST guard `test_telemetry_no_direct_posthog.py` is shipped and green. Plan 116 references it, never re-implements it.
- **Adding a new CI job for privacy gate:** D-09 is explicit — add PRIV-04 tests to the existing `tests` job. No `ci.yml` changes.
- **Checking `certifi.where()` explicitly in the selftest:** Do not add `certifi` to the spec or probe `SSL_CERT_FILE`. The hook auto-bundles `cacert.pem`; the clean-VM run is the proof. The selftest simply makes a real POST — if it lands, SSL is working.
- **Writing config.pkl in the selftest:** The selftest must snapshot/restore `_enabled` in-memory exactly like the `__main__` probe in `desktop/telemetry.py:1736-1748`. Never write to `config.pkl` from a headless self-test.
- **Sleeping more than ~2 seconds:** The `requests.post(timeout=2.0)` in `_drain_posthog_queue` is the binding deadline. `sleep(1.5)` is sufficient for one drain cycle after `run_selftest()`.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| SSL CA bundle in frozen exe | Manual `collect_data_files` in spec | `hook-certifi.py` (auto-discovered by PyInstaller 6.x) | Already active; `cacert.pem` confirmed in `dist/GenizahSearchPro/_internal/certifi/` |
| Pre-consent test fixture | New fixture | Copy the autouse pattern from `test_telemetry_review_fixes.py` line 28-56 | Pattern is proven; `ph._event_queue.get(timeout=1.0)` captures payloads reliably |
| Transport drop-counter monitoring | Custom monitoring code | `shared.posthog_server.get_dropped_event_count()` + `web.api_hardening.get_dropped_event_count()` | Both exist; runbook documents them per Phase-98 CLAUDE.md note |

---

## PyInstaller SSL/Certifi Bundling — Detailed Findings

This is the highest-value unknown per the research brief. All findings are VERIFIED.

### How certifi gets into the frozen exe [VERIFIED: inspected dist/ + hooks]

1. PyInstaller 6.19.0 + `pyinstaller-hooks-contrib` 2026.3 auto-discovers `hook-certifi.py` at `_pyinstaller_hooks_contrib/stdhooks/hook-certifi.py`.
2. That hook calls `collect_data_files('certifi')`, which bundles `certifi/cacert.pem` (272,441 bytes) into the `_internal/certifi/` subdirectory of the distribution.
3. **Confirmed in the existing build:** `dist/GenizahSearchPro/_internal/certifi/cacert.pem` exists.
4. `GenizahSearchPro.spec` has NO explicit `certifi` entry — this is correct; the auto-hook handles it. No spec change needed.

### How `requests` resolves the CA bundle at runtime when frozen [VERIFIED: source inspection]

The chain: `requests.certs.where()` → `certifi.where()`. In Python 3.11 (which this project uses), `certifi.where()` calls `importlib.resources.as_file(files("certifi").joinpath("cacert.pem"))`. When frozen under PyInstaller, `importlib.resources` resolves via `sys._MEIPASS` (the extracted temp folder). The path becomes `<_MEIPASS>/certifi/cacert.pem` — exactly where `hook-certifi.py` puts it.

`requests.adapters.HTTPAdapter.send()` calls `extract_zipped_paths(DEFAULT_CA_BUNDLE_PATH)`. `DEFAULT_CA_BUNDLE_PATH` comes from `certifi.where()` — the path already exists on disk in `_MEIPASS`, so `extract_zipped_paths` returns it unchanged. SSL verification proceeds with the bundled CA store.

**No `SSL_CERT_FILE` or `REQUESTS_CA_BUNDLE` env vars are needed.** The standard chain works transparently in the frozen binary.

### What the `--telemetry-selftest` SSL probe actually tests [VERIFIED: code inspection]

The selftest emits one `desktop_selftest` event, waits 1.5s, and checks the drop counter. A drop means either no key (handled by `if not api_key: continue`) OR a transport exception (the `requests.post` raised and was swallowed). On a clean machine with the key present and network up, zero drops + `SSL_OK` proves:
1. The key is embedded and resolved.
2. `requests.post` reached `https://eu.i.posthog.com/capture` over TLS.
3. `certifi/cacert.pem` was found (if SSL cert verify failed, `requests` would raise `SSLError` → caught → dropped → `SSL_FAIL`).

On a machine with no network, `requests.post(timeout=2.0)` raises `ConnectionError` → swallowed → event dropped. The selftest exits `SSL_FAIL` (drop count > 0). For the **offline arm** (D-05), this is intentional: the test is "app doesn't hang or crash," not "event delivered." The `--telemetry-selftest-offline` flag arm can simply skip the POST and check that the consent gate + scrubber chain complete without error.

### Spec change required? [VERIFIED: no]

`certifi` is already bundled via the auto-hook. The existing spec needs zero changes for telemetry SSL to work.

---

## Common Pitfalls

### Pitfall 1: Confusing "event dropped" with "SSL failure"
**What goes wrong:** The drop counter increments both when there is no API key AND when a `requests.post` raises. In the selftest, if `_TELEMETRY_KEY_DEFAULT` is missing (not baked), drops look like an SSL problem.
**How to avoid:** The selftest must call `_wire_transport_config()` first. If the key is the sentinel, the test should print `NO_KEY` (not `SSL_FAIL`) as a separate diagnostic.
**Warning signs:** Drop count > 0 even with network up.

### Pitfall 2: Writing config.pkl from the selftest block
**What goes wrong:** The `set_consent(True)` public API writes to `config.pkl`, persisting telemetry consent for the end user who ran `--telemetry-selftest` in developer mode.
**How to avoid:** Mirror the `desktop/telemetry.py:1736-1748` `__main__` probe exactly: take the `_enabled_lock`, set `_enabled = True` in-memory, restore in `finally`. Never call the public `set_consent()` from the selftest block.
**Warning signs:** `config.pkl` modified after a selftest run.

### Pitfall 3: The offline arm returning too slowly
**What goes wrong:** The selftest with `--telemetry-selftest-offline` (or with network off) hangs for 2.0 seconds per drain iteration waiting for `requests.post(timeout=2.0)` to time out, then the `sleep(1.5)` adds more. Total: up to ~3.5 seconds.
**How to avoid:** For the offline arm, skip the `sleep` entirely and just verify the chain (consent gate → scrubber → queue put) without waiting for drain. The `OFFLINE_OK` token documents this scope.
**Warning signs:** Selftest takes > 5 seconds on a machine with firewall rules.

### Pitfall 4: Pre-consent test only covers `track()`, not all three entry points
**What goes wrong:** D-02 requires ALL three public entry points (`track()`, `track_performance()`, `track_error()`) to respect the consent gate. The existing `test_no_events_before_consent` (line 74) only calls `track()`.
**How to avoid:** The new test in `test_telemetry_priv04.py` must call all three and assert the queue remains empty after each.
**Warning signs:** The test passes but `track_error()` has a bug that allows emission before consent — undetected.

### Pitfall 5: Amending REQUIREMENTS.md INFRA-06 without a dated note
**What goes wrong:** The amendment looks like a silent history rewrite. The 2026-06-14 shared-project reversal is a documented decision that future maintainers need context for.
**How to avoid:** Append a dated note referencing `.planning/research/POSTHOG-PROJECT-DECISION.md` inline (not just change the text). Keep the old wording struck through or in a comment so the history is visible.

### Pitfall 6: DOCUMENTATION_INDEX.md "Last updated" date not updated
**What goes wrong:** The index shows "Last updated: 2026-03-26" after the runbook is added, making it appear stale.
**How to avoid:** Bump the "Last updated" timestamp in `docs/DOCUMENTATION_INDEX.md` as part of the same commit.

---

## Code Examples

### PRIV-04 Forbidden-input test — canonical shape

```python
# Source: extends tests/test_telemetry_review_fixes.py + test_telemetry_consent_gate.py patterns
def test_priv04_my_library_path_not_in_payload():
    """SC#2: a Windows My-Library path on a forbidden key must never reach enqueue_event."""
    import desktop.telemetry as tel

    tel.set_consent(True)
    # Simulate what a My-Library search producer might accidentally pass
    tel.track(
        tel.DesktopEvent.SEARCH_EXECUTED,
        search_mode='keyword',
        corpus_scope='local',
        path=r'C:\Users\gersh\Library\teshuvot.pdf',  # forbidden key
    )

    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']

    assert 'path' not in props, (
        "My-Library path key 'path' must be dropped by scrubber before enqueue_event"
    )
    # Allowed props survive
    assert props.get('search_mode') == 'keyword'
    assert props.get('corpus_scope') == 'local'
```

### PRIV-04 Pre-consent zero-emit — all three entry points

```python
def test_priv04_pre_consent_zero_emit_all_entry_points():
    """D-02: track() / track_performance() / track_error() all gate on consent."""
    import desktop.telemetry as tel

    # Fixture leaves consent = False (fresh empty fake_config)
    assert not tel.is_enabled()

    tel.track(tel.DesktopEvent.SELFTEST)
    tel.track_performance(tel.DesktopEvent.SESSION_PERF, duration_ms=100.0)
    tel.track_error('ctx', ValueError('test'))

    assert ph._event_queue.empty(), (
        "No event must be enqueued before consent — all three entry points "
        "must respect is_enabled() gate (CONSENT-01 / D-02)"
    )
```

### `--telemetry-selftest` block placement in genizah_app.py

```python
# Placement: in genizah_app.py, BEFORE --self-test-pymupdf, BEFORE QApplication construction
if "--telemetry-selftest" in sys.argv:
    import desktop.telemetry as _tel
    import shared.posthog_server as _ph
    import time as _time

    _offline = "--telemetry-selftest-offline" in sys.argv
    _prior_enabled = _tel.is_enabled()
    try:
        with _tel._enabled_lock:
            _tel._enabled = True   # in-memory only — no config.pkl write (D-04 WR-03 pattern)
        _tel._wire_transport_config()
        if _offline:
            # Offline arm: verify chain runs without crash/dialog; skip network POST
            _tel.run_selftest()
            # No sleep — just verify enqueue succeeded
            print("OFFLINE_OK")
            sys.exit(0)
        # Online arm: emit + wait for drain thread POST attempt
        _tel.run_selftest()
        _time.sleep(1.5)
        _dropped = _ph.get_dropped_event_count()
        if _dropped > 0:
            print(f"SSL_FAIL: {_dropped} events dropped (no key, SSL error, or no network)",
                  file=sys.stderr)
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

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Separate desktop PostHog project (isolated) | ONE shared web project (id 134161, EU) + `platform=desktop` namespace | 2026-06-14 reversal | INFRA-06 requirement wording is stale; runbook must reflect actual posture |
| Phase 116 slot for AST guard delivery | PRIV-03 delivered early in Phase 111-03 | 2026-06-11 | Phase 116 references, never re-implements |
| Transport key from `POSTHOG_API_KEY` env only | Embedded `_TELEMETRY_KEY_DEFAULT` (baked at build) + `GENIZAH_TELEMETRY_KEY` override | 2026-06-15 | Frozen .exe emits without any env var; selftest must call `_wire_transport_config()` first |
| Two separate PostHog queues (Phase-98 design) | Still two queues (`web.api_hardening` + `shared.posthog_server`) | Phase 98 (2026-05-25) | Runbook must monitor BOTH drop counters, as noted in CLAUDE.md |

**Deprecated/outdated:**
- INFRA-06 wording "isolated from the web project" in REQUIREMENTS.md — stale since 2026-06-14 reversal; Phase 116 amends it.

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | The clean-VM UAT (D-06) also closes Phase 114's still-open "live PostHog event delivery" UAT item | Validation Architecture | If Phase 114's UAT needs a separate sign-off, an additional HUMAN-UAT checklist item is needed — but the STATE.md confirms both items are resolved by the same run |

**All other claims in this research were verified via code inspection or direct tool verification.**

---

## Open Questions (RESOLVED)

> Both resolved inline with concrete recommendations; the Phase 116 plans implement them
> (116-02: `SSL_FAIL` for the no-key/drop path; offline arm skips the POST, network-disabled
> run is the D-06 HUMAN-UAT).

1. **Output token when key is sentinel (not baked)**
   - What we know: `_TELEMETRY_KEY_DEFAULT` is baked with the real `phc_` key as of 2026-06-15.
   - What's unclear: what should `--telemetry-selftest` print if run on a dev build where the key is still the sentinel? `NO_KEY`? `SSL_FAIL`?
   - Recommendation: print `SSL_FAIL: no phc_ key configured` with exit(1). This is a dev-build misconfiguration, not a runtime SSL failure. The distinction is irrelevant to a shipped .exe which always has the key baked.

2. **Should `--telemetry-selftest-offline` actually disable the network?**
   - What we know: The offline arm is about proving the app doesn't crash/hang, not about simulating real network failure.
   - What's unclear: does the plan need to block the network in any automated way, or is it purely a manual step?
   - Recommendation: The offline arm flag skips the POST (verifies chain without I/O) for automated assertion; the actual "network disabled" run is the HUMAN-UAT item (D-06). No automated network-blocking is needed.

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.11 | Tests | ✓ | 3.11.x | — |
| `requests` | `posthog_server.py` transport | ✓ | 2.32.5 | — |
| `certifi` | SSL CA bundle | ✓ | 2026.2.25 | — |
| PyInstaller | Frozen binary build | ✓ | 6.19.0 | — |
| `pyinstaller-hooks-contrib` | `hook-certifi.py` auto-bundle | ✓ | 2026.3 | — |
| Clean Windows VM (no Python) | SC#3 / D-06 HUMAN-UAT | ✗ (not available as automated step) | — | No fallback — must be run manually once before milestone close |
| `dist/GenizahSearchPro/_internal/certifi/cacert.pem` | SSL verification in frozen binary | ✓ (in existing build) | 272,441 bytes | — |

**Missing dependencies with no fallback:**
- Clean Windows VM with no Python installed — required for D-06 HUMAN-UAT. Not a blocker for automated tasks; blocks final milestone-close sign-off.

---

## Validation Architecture

> `workflow.nyquist_validation` is `true` in `.planning/config.json` — this section is required.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (CI-pinned, unlocked version via `pip install pytest`) |
| Config file | none (no pytest.ini or pyproject.toml test config) |
| Quick run command | `pytest tests/test_telemetry_priv04.py -x` |
| Full telemetry suite | `pytest tests/test_telemetry*.py tests/test_no_direct*.py tests/test_no_dynamic*.py -m "not gui"` |
| Full test job command | `pytest tests/ -m "not gui"` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PRIV-03 | AST guard `test_telemetry_no_direct_posthog.py` green on both Ubuntu + Windows | static AST (stdlib) | `pytest tests/test_telemetry_no_direct_posthog.py` | ✅ already exists |
| PRIV-04 (D-01) | Windows My-Library path on `path` key dropped | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_my_library_path_not_in_payload -x` | ❌ Wave 0 |
| PRIV-04 (D-01) | `filename` key dropped | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_filename_key_dropped -x` | ❌ Wave 0 |
| PRIV-04 (D-01) | Hebrew query string on any allowed key scrubbed to `[REDACTED]` | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_hebrew_query_value_redacted -x` | ❌ Wave 0 |
| PRIV-04 (D-01) | Traceback with `frame_locals` and `traceback_raw` — both dropped | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_crash_forbidden_fields_dropped -x` | ❌ Wave 0 |
| PRIV-04 (D-01) | `hostname` and `username` keys dropped | unit/scrubber | `pytest tests/test_telemetry_priv04.py::test_priv04_hostname_username_dropped -x` | ❌ Wave 0 |
| PRIV-04 (D-02) | Pre-consent: `track()` + `track_performance()` + `track_error()` → zero events | unit/consent-gate | `pytest tests/test_telemetry_priv04.py::test_priv04_pre_consent_zero_emit_all_entry_points -x` | ❌ Wave 0 |
| SC#1 / CI | `tests` job passes on ubuntu-latest + windows-latest | CI verification | `.github/workflows/ci.yml` — existing, no change | ✅ already satisfied |
| SC#3 | `--telemetry-selftest` prints `SSL_OK` on clean Windows VM | HUMAN-UAT | manual: `.\GenizahSearchPro.exe --telemetry-selftest` on clean VM | ❌ Wave 0 (code); HUMAN-UAT (execution) |
| SC#3 / D-05 | `--telemetry-selftest-offline` prints `OFFLINE_OK` without hanging | smoke/manual | manual or: `pytest` verifies code path; offline assertion is structural | ❌ Wave 0 (code) |
| INFRA-06 | Runbook `docs/guides/TELEMETRY_RUNBOOK.md` exists with required sections | documentation | human review of file content | ❌ Wave 0 |
| INFRA-06 | REQUIREMENTS.md INFRA-06 "isolated" wording amended | documentation | `grep "isolated" .planning/REQUIREMENTS.md` → returns nothing in that section | ❌ Wave 0 |
| D-10 | ~290 telemetry tests documented as milestone-exit gate | documentation | human review of VERIFICATION.md section | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `pytest tests/test_telemetry_priv04.py -x`
- **Per wave merge:** `pytest tests/test_telemetry*.py tests/test_no_direct*.py tests/test_no_dynamic*.py -m "not gui"`
- **Phase gate (before `/gsd:verify-work`):** `pytest tests/ -m "not gui"` (full non-GUI suite) on both Ubuntu and Windows via CI

### Wave 0 Gaps

- [ ] `tests/test_telemetry_priv04.py` — covers PRIV-04 D-01 forbidden-field scenarios (6 test functions) + D-02 pre-consent zero-emit (1 test function); autouse fixture copied from `test_telemetry_review_fixes.py`
- [ ] `genizah_app.py __main__` — `--telemetry-selftest` / `--telemetry-selftest-offline` block added (~30 lines)
- [ ] `docs/guides/TELEMETRY_RUNBOOK.md` — new file (~200-300 lines, 5 sections per D-08)
- [ ] `.planning/REQUIREMENTS.md` — INFRA-06 "isolated project" wording amended with dated note
- [ ] `docs/DOCUMENTATION_INDEX.md` — 1-line runbook entry + timestamp update
- [ ] VERIFICATION.md section — documents the telemetry regression suite as milestone-exit gate

**Existing test infrastructure that needs NO changes:** All 234 telemetry tests from Phases 111-115 already pass; CI job already runs both OSes; PRIV-03 AST guard already green.

### What Automated Tests Prove vs. Clean-VM Run

| Claim | Automated Test Proves | Only Clean-VM Run Proves |
|-------|----------------------|--------------------------|
| Scrubber drops forbidden keys structurally | ✅ `test_telemetry_priv04.py` (pure function tests) | — |
| No emission before consent | ✅ D-02 test | — |
| AST guard blocks chokepoint bypass | ✅ `test_telemetry_no_direct_posthog.py` | — |
| `certifi/cacert.pem` is in the frozen binary | — | ✅ D-06 HUMAN-UAT |
| SSL chain works in the frozen binary (not borrowed from dev Python) | — | ✅ D-06 HUMAN-UAT |
| Offline degradation: no crash, no dialog, fast return | Structural: `--telemetry-selftest-offline` exits cleanly | ✅ D-06 HUMAN-UAT (with network disabled) |
| Real PostHog event delivery end-to-end | — | ✅ D-06 HUMAN-UAT (closes Phase 114 UAT) |

---

## Security Domain

> `security_enforcement` not set in `.planning/config.json` → treated as enabled.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | — |
| V3 Session Management | no | — |
| V4 Access Control | no | — |
| V5 Input Validation | yes (scrubber) | `_scrub_props` / `_validate_props` structural filter; `_BANNED_KEYS` exact match |
| V6 Cryptography | partial | SSL via certifi; no hand-rolled crypto; PostHog uses TLS 1.2+ |

### Known Threat Patterns for This Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Forbidden-field payload escape | Information Disclosure | Structural scrubber `_scrub_props` + allowlist `_ALLOWED_PROPS`; AST guard prevents chokepoint bypass (PRIV-03) |
| Pre-consent emission | Tampering | `is_enabled()` consent gate in every public entry point; D-02 test proves it |
| Embedded key exposure (embedded in binary) | Information Disclosure | Publishable write-only ingest key — abuse-tolerant by PostHog design; rotation procedure documented in runbook (D-08b) |
| SSL spoofing in frozen binary | Spoofing | certifi CA bundle auto-bundled by hook; no user-supplied CA path accepted |

---

## Sources

### Primary (HIGH confidence)
- `desktop/telemetry.py` — inspected directly: `_scrub_props` (:245), `_BANNED_KEYS` (:220), `_ALLOWED_PROPS` (:295), `run_selftest()` (:836), `__main__` probe (:1729), `_wire_transport_config` (:411)
- `shared/posthog_server.py` — inspected directly: `get_dropped_event_count()` (:88), `requests.post(timeout=2.0)` (:258), `_drain_and_discard` (:276), `_flush_before_exit` (:291)
- `tests/test_telemetry_scrubbing.py`, `tests/test_telemetry_review_fixes.py`, `tests/test_telemetry_consent_gate.py` — inspected directly; autouse fixture pattern confirmed
- `tests/test_telemetry_no_direct_posthog.py` — PRIV-03 guard confirmed shipped and complete
- `GenizahSearchPro.spec` — no certifi entry (auto-hook handles it); confirmed
- `_pyinstaller_hooks_contrib/stdhooks/hook-certifi.py` — `collect_data_files('certifi')` confirmed
- `dist/GenizahSearchPro/_internal/certifi/cacert.pem` — confirmed present (272,441 bytes)
- `.github/workflows/ci.yml` — `tests` job confirmed: `pytest tests/ -m "not gui"` on ubuntu-latest + windows-latest
- `genizah_app.py:27486` — `--self-test-pymupdf` template confirmed

### Secondary (MEDIUM confidence)
- PyInstaller 6.19.0 frozen-binary certifi resolution behavior (verified via `certifi.core` source + `requests.adapters.extract_zipped_paths` source inspection)
- `requests.certs.where()` → `certifi.where()` chain confirmed via source

---

## Metadata

**Confidence breakdown:**
- PRIV-04 test pattern: HIGH — existing fixture patterns directly inspected; scrubber is pure-function
- `--telemetry-selftest` design: HIGH — exact template at genizah_app.py:27486 confirmed; `run_selftest()` + `__main__` probe confirmed
- SSL/certifi bundling: HIGH — cacert.pem confirmed in dist; hook confirmed; `certifi.where()` frozen-path resolution confirmed via source
- CI structure: HIGH — ci.yml inspected directly
- Runbook content: HIGH — both drop counters confirmed; key-resolution order confirmed; all referenced surfaces verified

**Research date:** 2026-06-16
**Valid until:** 2026-07-16 (30 days; stable tech stack)
