---
phase: 111
slug: telemetry-foundation
status: verified
threats_open: 0
asvs_level: 1
created: 2026-06-14
---

# Phase 111 — Security (Telemetry Foundation)

> Per-phase security contract: threat register, accepted risks, and audit trail.
> Scope: opt-in PostHog desktop telemetry foundation — transport layer extensions
> (`shared/posthog_server.py`) + desktop chokepoint (`desktop/telemetry.py`) +
> structural CI guard (`tests/test_telemetry_no_direct_posthog.py`).

---

## Trust Boundaries

| Boundary | Description | Data Crossing |
|----------|-------------|---------------|
| caller → `enqueue_event` | Properties dicts from desktop callsites may carry PII (query text, My Library paths, Hebrew content); optional scrub hook is the shared-module defence-in-depth checkpoint | Potentially-sensitive property dicts |
| `desktop/` callsites → `desktop/telemetry.py` | Untrusted property dicts; consent gate, scrubber, and allowlist are all enforced before anything reaches the transport | Potentially-sensitive event + property data |
| `desktop/telemetry.py` → `shared.posthog_server.enqueue_event` | The only sanctioned egress from desktop; everything past here is already gated, scrubbed, and allowlisted (PRIV-03) | Scrubbed, allowlisted event payloads |
| process → `eu.i.posthog.com/capture` (or `_host_override`) | Outbound HTTP; the only network egress; `_flush_before_exit` adds a synchronous POST path | Pseudonymous telemetry events |
| desktop config → transport key | Desktop sets its capture key via `set_capture_api_key` (process-local global), never via `os.environ`, so it cannot leak into / overwrite the web server's `POSTHOG_API_KEY` | API key (publishable, write-only) |
| `config.pkl` (disk) | Stores consent flag + pseudonymous install uuid4 + the identified Supabase user.id | Consent + pseudonymous identity |
| future `desktop/*.py` callsites → transport | A developer in Phases 112-115 could accidentally bypass the consent gate; the PRIV-03 AST guard is the structural backstop (T-111-15/19) | n/a — structural boundary |

---

## Threat Register

| Threat ID | Category | Component | Disposition | Mitigation | Status |
|-----------|----------|-----------|-------------|------------|--------|
| T-111-01 | Information Disclosure | `enqueue_event` scrub-hook ordering | mitigate | `_scrub_hook` called BEFORE `_event_queue.put_nowait` — raw data never enters the queue (`posthog_server.py:197-208`) | closed |
| T-111-02 | Information Disclosure | scrub-hook exception path | mitigate | `except Exception: return` on hook call — fail-closed, drops event rather than sending raw (`posthog_server.py:205-206`) | closed |
| T-111-03 | Tampering | global consent gate creep into shared module | accept→mitigate | No `_telemetry_enabled` global added to `shared/posthog_server.py`; `enqueue_event` signature unchanged; verified by grep returning 0 matches (`posthog_server.py` entire file) | closed |
| T-111-04 | Denial of Service | `_flush_before_exit` hangs at crash time | mitigate | TRUE wall-time deadline: `remaining = deadline - time.monotonic()` before each POST; stops POSTing (drain-only) once `remaining <= 0`; per-POST `timeout=min(remaining, 2.0)` (`posthog_server.py:287-309`) | closed |
| T-111-05 | Repudiation | already-queued events sent after opt-out | mitigate | `_drain_and_discard()` empties queue with zero `requests.post` calls (`posthog_server.py:257-269`); called by `set_consent(False)` (`telemetry.py:411`) | closed |
| T-111-06 | Information Disclosure | PII via traceback/frame-locals reaching PostHog | mitigate | `_scrub_props` drops `_BANNED_KEYS` by exact/token match so `context` survives (`telemetry.py:160-231`); `_PATH_RE` + `_HEBREW_TEXT_RE` redact values (`telemetry.py:142-154`); 500-char cap; `track_error` emits only `type(exc).__name__` never `str(exc)` (`telemetry.py:546`) | closed |
| T-111-07 | Information Disclosure | property/event name bypassing allowlist/enum | mitigate | `track()` validates against `_VALID_EVENT_VALUES` then rejects `_TRACK_FORBIDDEN_EVENTS` (`telemetry.py:487-492`); `_validate_props` drops non-`_ALLOWED_PROPS` keys (`telemetry.py:257-265`) | closed |
| T-111-08 | Repudiation | events firing before consent | mitigate | `is_enabled()` returns False on absent key; every public callable calls `if not is_enabled(): return` at entry (`telemetry.py:475,511,542,566,598,622`); uuid minted only inside `set_consent(True)` when `_install_id` is falsy (`telemetry.py:386-388`) | closed |
| T-111-09 | Information Disclosure | email/name leaking on identify | mitigate | `identify()` props contain only `$process_person_profile` + `$anon_distinct_id` + base props; no email/name keys constructed or passed (`telemetry.py:571-574`); `email` and `name` are also in `_BANNED_KEYS` (`telemetry.py:164`) | closed |
| T-111-10 | Privacy | install id derived from hardware (MAC) | mitigate | `uuid.uuid4().hex` only (`telemetry.py:387`); no `uuid1()` call anywhere in the file | closed |
| T-111-11 | Repudiation | install-id deletion on opt-out breaks continuity | accept→mitigate | `set_consent(False)` branch does NOT include `TELEMETRY_INSTALL_ID_KEY` in the `updates` dict; comment documents the invariant (`telemetry.py:401`); in-memory `_install_id` retained | closed |
| T-111-12 | Tampering | embedded publishable key abuse | accept | Reused web publishable (`phc_`) key is write-only and already public in the web JS bundle; NEVER a personal `phx_` key; abuse-tolerant; rotation documented for Phase 116. Documented in Accepted Risks Log below. | closed |
| T-111-13 | Denial of Service | consent gate raising during a crash hook | mitigate | `is_enabled()` wrapped in `try/except Exception: return False` (`telemetry.py:342-346`); cached in-memory bool under `_enabled_lock` | closed |
| T-111-14 | Tampering | self-test firing in normal use | mitigate | `run_selftest()` gate-checks `is_enabled()` first (`telemetry.py:622`); `__main__` block gated on `GENIZAH_TELEMETRY_KEY` env var (`telemetry.py:724`); not callable at normal import time | closed |
| T-111-15 | Tampering / Information Disclosure | desktop callsite bypassing chokepoint to reach `enqueue_event` | mitigate | Absolute AST guard (`test_telemetry_no_direct_posthog.py:148-181`) scans all `desktop/*.py` by resolved path; exempts ONLY `desktop/telemetry.py`; synthetic tests confirm bare + aliased (`import as ph`, `from shared import posthog_server as ph`) forms detected | closed |
| T-111-16 | Repudiation | vacuous (always-green) guard giving false assurance | mitigate | `test_lint_rejects_synthetic_violation` (line 204), `test_lint_detects_aliased_import_call` (line 224), `test_lint_detects_from_shared_import_alias` (line 252) all exercise visitor on known-bad source; `test_chokepoint_itself_does_import_posthog` (line 184) confirms exemption targets a real importer | closed |
| T-111-17 | Tampering | desktop key mutating web's `POSTHOG_API_KEY` env | mitigate | `set_capture_api_key` sets `_api_key_override` process-local global only, no `os.environ` write (`posthog_server.py:116-129`); transport resolves `(_api_key_override or os.environ.get('POSTHOG_API_KEY',''))` (`posthog_server.py:145-149`); grep confirms no `os.environ[` write in `posthog_server.py` | closed |
| T-111-18 | Spoofing / Tampering | `$identify` emitted via generic `track()` without `$anon_distinct_id` | mitigate | `track()` rejects `$identify` via `_TRACK_FORBIDDEN_EVENTS` (`telemetry.py:129-135,491`); only `identify()` calls `enqueue_event(DesktopEvent.IDENTIFY.value, ...)` directly (`telemetry.py:582`) with `$anon_distinct_id` set | closed |
| T-111-19 | Tampering | over-broad basename exemption letting future `desktop/widgets/telemetry.py` bypass guard | mitigate | Exemption uses `path.resolve() == CHOKEPOINT` where `CHOKEPOINT = (DESKTOP_DIR / 'telemetry.py').resolve()` (`test_telemetry_no_direct_posthog.py:34,162`); `test_skip_is_by_resolved_path_not_basename` (line 280) pins this | closed |
| T-111-SC | Tampering | npm/pip/cargo supply-chain installs | mitigate | Zero new third-party packages in all three plans (all SUMMARY `tech-stack.added: []`); `desktop/telemetry.py` imports only stdlib + `genizah_core`/`version`/`shared.posthog_server`; `shared/posthog_server.py` adds only stdlib (`time`, `typing.Callable`); `requests` was pre-existing | closed |

*Status: open · closed*
*Disposition: mitigate (implementation required) · accept (documented risk) · transfer (third-party)*

---

## Accepted Risks Log

| Risk ID | Threat Ref | Rationale | Accepted By | Date |
|---------|------------|-----------|-------------|------|
| AR-111-01 | T-111-12 | The embedded desktop publishable key (`phc_...`) is write-only (capture-only, no read access) and is already public in the web JS bundle. PostHog publishable keys are designed to be public. The key is NOT a personal secret key (`phx_`). Abuse (inflating fake events) is tolerable — PostHog provides project-level dashboards and the data is internal analytics only, not user-facing data. Key rotation procedure is documented in INFRA-06 (Phase 116). Acceptance criteria: key type is `phc_` (publishable), never `phx_` (personal); rotation plan exists. | gsd-security-auditor / Hillel Gershuni | 2026-06-14 |

*Accepted risks do not resurface in future audit runs.*

---

## Security Audit Trail

| Audit Date | Threats Total | Closed | Open | Run By |
|------------|---------------|--------|------|--------|
| 2026-06-14 | 20 | 20 | 0 | gsd-security-auditor (claude-sonnet-4-6) |

---

## Sign-Off

- [x] All threats have a disposition (mitigate / accept / transfer)
- [x] Accepted risks documented in Accepted Risks Log (T-111-12 / AR-111-01)
- [x] `threats_open: 0` confirmed
- [x] `status: verified` set in frontmatter

**Approval:** verified 2026-06-14

---

## Evidence Summary

### Test Suite

All 62 Phase 111 mitigation tests passed:

```
tests/test_telemetry_posthog_server_ext.py   — 18 tests (transport layer: T-111-01/02/04/05/17)
tests/test_telemetry_consent_gate.py         — 11 tests (consent gate: T-111-08/13/17)
tests/test_telemetry_scrubbing.py            —  9 tests (scrubber: T-111-06)
tests/test_telemetry_allowlist.py            —  8 tests (allowlist/enum: T-111-07/18)
tests/test_telemetry_identity.py             —  9 tests (identity: T-111-09/10/11/14/18)
tests/test_telemetry_no_direct_posthog.py    —  6 tests (AST guard: T-111-15/16/19)
Total: 62 passed in 2.56s
```

### Key Code Citations

| Threat | Control | File:Line |
|--------|---------|-----------|
| T-111-01 | Scrub hook before `put_nowait` | `shared/posthog_server.py:197-208` |
| T-111-02 | `except Exception: return` (fail-closed) | `shared/posthog_server.py:205-206` |
| T-111-03 | No `_telemetry_enabled` global (grep:0) | `shared/posthog_server.py` entire |
| T-111-04 | `remaining = deadline - time.monotonic()` per-POST | `shared/posthog_server.py:295-309` |
| T-111-05 | `_drain_and_discard` — no `requests.post` | `shared/posthog_server.py:257-269` |
| T-111-05 | Called on opt-out | `desktop/telemetry.py:411` |
| T-111-06 | `_BANNED_KEYS` exact match + `_is_banned_key` | `desktop/telemetry.py:160-185` |
| T-111-06 | `_PATH_RE` + `_HEBREW_TEXT_RE` + 500-char cap | `desktop/telemetry.py:142-154, 199-204` |
| T-111-06 | `type(exc).__name__` only, no `str(exc)` | `desktop/telemetry.py:546` |
| T-111-07 | `_VALID_EVENT_VALUES` check then `_TRACK_FORBIDDEN_EVENTS` | `desktop/telemetry.py:487-492` |
| T-111-07 | `_validate_props` drops non-allowlisted keys | `desktop/telemetry.py:257-265` |
| T-111-08 | `if not is_enabled(): return` at 6 entry points | `desktop/telemetry.py:475,511,542,566,598,622` |
| T-111-08 | UUID minted only in `set_consent(True)` when absent | `desktop/telemetry.py:386-388` |
| T-111-09 | `identify()` props: no email/name keys | `desktop/telemetry.py:571-574` |
| T-111-10 | `uuid.uuid4().hex` only; no `uuid1()` | `desktop/telemetry.py:387` |
| T-111-11 | `TELEMETRY_INSTALL_ID_KEY` not in opt-out updates | `desktop/telemetry.py:401` |
| T-111-13 | `is_enabled()` in `try/except Exception: return False` | `desktop/telemetry.py:342-346` |
| T-111-14 | `run_selftest()` consent-gated | `desktop/telemetry.py:622` |
| T-111-14 | `__main__` block gated on `GENIZAH_TELEMETRY_KEY` | `desktop/telemetry.py:724` |
| T-111-15 | AST guard production scan, resolved-path exemption | `tests/test_telemetry_no_direct_posthog.py:148-181` |
| T-111-16 | Three synthetic-violation tests | `tests/test_telemetry_no_direct_posthog.py:204,224,252` |
| T-111-17 | `set_capture_api_key` sets `_api_key_override` only | `shared/posthog_server.py:116-129` |
| T-111-17 | Resolver: `(_api_key_override or os.environ.get(...))` | `shared/posthog_server.py:145-149` |
| T-111-18 | `_TRACK_FORBIDDEN_EVENTS` includes `$identify` | `desktop/telemetry.py:129-135` |
| T-111-18 | `identify()` sole emitter of `$identify` | `desktop/telemetry.py:582` |
| T-111-19 | `path.resolve() == CHOKEPOINT` (not basename) | `tests/test_telemetry_no_direct_posthog.py:34,162` |
