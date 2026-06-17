---
phase: 114
slug: usage-analytics
status: verified
threats_open: 0
asvs_level: 1
created: 2026-06-16
---

# Phase 114 — Security

> Per-phase security contract: threat register, accepted risks, and audit trail.
> Phase 114 wires desktop usage-analytics telemetry producers through the
> consent-gated `desktop/telemetry.py` chokepoint. The dominant threat class is
> PII / content / identity exfiltration across the desktop→PostHog boundary.

---

## Trust Boundaries

| Boundary | Description | Data Crossing |
|----------|-------------|---------------|
| desktop app → PostHog (network) | Every telemetry payload crosses here; dominant threat class is PII/content/identity exfiltration | Allowlisted event props only (enums, counts/buckets, hardcoded names, session uuid) |
| Supabase auth state → telemetry identity | The user identity crosses from the auth client into the telemetry `distinct_id` | `current_user._uuid` (raw Supabase UUID) — never `.id` (int hash), never email/name |
| desktop UI widgets/dialogs → telemetry payload | Translated combo/tab labels, PGP tag text, query-rich state, dialog titles (embed shelfmarks), QFileDialog paths sit here | Must never become a telemetry value |
| CI scan → producer source | The re-scoped D-17 AST guard is the structural enforcement boundary for value-side privacy + identity-source correctness | Static analysis of telemetry-call argument expressions |

---

## Threat Register

| Threat ID | Category | Component | Disposition | Mitigation | Status |
|-----------|----------|-----------|-------------|------------|--------|
| T-114-01 | Spoofing | `_sync_telemetry_identity` / login wiring | mitigate | Identity uses `current_user._uuid`, never `.id`. `genizah_app.py:3598` = `identify(user._uuid)`; grep for `identify(...\.id)` → 0 callsites; D-17 AST identity-callsite check (LOW-10). | closed |
| T-114-02 | Information Disclosure | session_start props | mitigate | Props limited to `session_id`, `ui_language`, `python_version`, `pyqt_version` + `_BASE_PROPS()` (`platform`, `app_version`, `os_family`, `os_version`); no hostname/username/path/cwd. `test_coordinator_session_start_props_allowlisted`. | closed |
| T-114-03 | Information Disclosure | event before consent OR before coordinator | mitigate | All emission routes through `telemetry.track()`/`identify()`/`reset_identity()` (each `is_enabled()`-gated); coordinator early-returns if not enabled; `_telemetry_ready()` gates every usage producer — **including the two `desktop/result_dialog.py` callsites, now routed through the host's gated `_emit_feature_opened()` (WR-01 fix, commit `5c6970c8`)** so no orphan empty-`session_id` event can fire in the ~700ms startup window. | closed |
| T-114-04 | Tampering / Repudiation | stale persisted identity / re-opt-in | mitigate | `_sync_telemetry_identity` rechecks `current_user` liveness each run; stale `IDENTIFIED_USER_KEY` → `reset_identity()`; mid-session opt-out→opt-in re-identifies. Runs unconditionally before the `_telemetry_session_started` one-shot guard. `test_reopt_in_reidentifies_without_second_session_start`. | closed |
| T-114-05 | Information Disclosure | tab_name value | mitigate | `tab_name` from hardcoded `_TAB_NAME_MAP`, never `tabText()`; programmatic tab changes suppressed via `_programmatic_tab_change`/`_restoring_session` in `_set_active_tab`. D-17 AST guard enforces no-`tabText`. | closed |
| T-114-06 | Information Disclosure | search_mode / corpus_scope value | mitigate | `search_mode` from static `_SEARCH_MODE_ENUM`/`_COMP_SEARCH_MODE_ENUM` + literal `'pgp_tags'`; `corpus_scope` from `currentData()` (fixed code), never `currentText()`. | closed |
| T-114-07 | Information Disclosure | query / PGP tag / exclusion text reaching event | mitigate | All three emit helpers build props only from mode + corpus + `action` + `session_id` + `_telemetry_result_bucket(count)`. Query string, comp source text, PGP `tag` arg, filters, exclusion list structurally absent; `result_count` coarse-bucketed. | closed |
| T-114-08 | Repudiation / Tampering | double-counted / ghost search events | mitigate | Per-run `emitted` idempotency guard (regular/PGP-tags/composition); `_app_shutting_down` first-line guard in all three emit helpers; set at top of `closeEvent` before teardown. | closed |
| T-114-09 | Information Disclosure | feature_name / dialog_name / action value | mitigate | Hardcoded constants only (`joins_lab`, `fragment_puzzle`, `fjms_catalog`, `result_detail`, `visual_similarity`, `export`, `_EXPORT_ACTION_BY_FMT` static map); never `windowTitle()`/`selectedFiles()`; dead VS dialog not instrumented. D-17 AST guard. | closed |
| T-114-10 | Information Disclosure | heartbeat payload | mitigate | `desktop_active_ping` carries only `session_id` (uuid4 hex) — no content, no env identifiers. | closed |
| T-114-11 | Repudiation / Tampering | fabricated DAU | mitigate | Heartbeat five-guarded: `_telemetry_ready()`, `is_enabled()`, exclude session_start UTC-day, once-per-UTC-day, `ApplicationActive`; driven by `applicationStateChanged` (not a naive 24h timer). | closed |
| T-114-12 | Information Disclosure | future producer leak / identity-source drift (regression) | mitigate | Re-scoped D-17 AST guard (`tests/test_no_dynamic_telemetry_strings.py`) fails CI if any telemetry-call argument calls a forbidden accessor OR any `identify()` callsite passes a non-`_uuid` source; production scan green across `genizah_app.py`, `gui_threads.py`, `desktop/result_dialog.py`. | closed |
| T-114-13 | Repudiation / Tampering | ghost / double-counted feature_opened | mitigate | VS emits only from live `source in ('visual','combined')` branch (mutually exclusive with `joins_lab`); puzzle ×2 paths = distinct gestures; FJMS ×2 = distinct surfaces. | closed |
| T-114-14 | Information Disclosure | usage event before `_session_id` / identity correction | mitigate | Every usage producer short-circuits `if not self._telemetry_ready(): return` before building any payload. | closed |
| T-114-15 | Repudiation / Tampering | counting non-events (no-data export, cancelled save) | mitigate | `export` dialog emit after the no-data early-return + before the save dialog; `action='export_*'` emit only after a path is chosen. | closed |
| T-114-SC | Tampering | package installs | accept | No package installs in Phase 114 (pure wiring). `git diff a7545048..HEAD` shows zero changes to `requirements*.txt`/`setup.py`/`pyproject.toml`. | closed |

*Status: open · closed*
*Disposition: mitigate (implementation required) · accept (documented risk) · transfer (third-party)*

---

## Accepted Risks Log

| Risk ID | Threat Ref | Rationale | Accepted By | Date |
|---------|------------|-----------|-------------|------|
| AR-114-01 | T-114-SC | Phase 114 is pure producer-wiring with zero package installs; no supply-chain surface introduced. No legitimacy checkpoint required. | Hillel Gershuni | 2026-06-16 |

*Accepted risks do not resurface in future audit runs.*

---

## Security Audit Trail

| Audit Date | Threats Total | Closed | Open | Run By |
|------------|---------------|--------|------|--------|
| 2026-06-16 | 16 | 15 (verify) + 1 (accept) | 0 | gsd-security-auditor (sonnet) + orchestrator |

**Notes:**
- Register was authored at plan time (`register_authored_at_plan_time: true`) across all three PLAN.md `<threat_model>` blocks — auditor ran in verify-mitigations mode.
- Initial audit returned `OPEN_THREATS` with 1 open (T-114-03 partial / WR-01: `desktop/result_dialog.py` emissions bypassed the `_telemetry_ready()` gate — WARNING, not a privacy blocker). User chose **fix**; resolved in commit `5c6970c8` (both callsites routed through the gated `_emit_feature_opened()` host helper); re-verified closed.
- WR-04 (consent disclosure text said `user.id`; code sends `_uuid`) — already fixed pre-audit in commit `f7bf67e4` (EN+HE reworded to "Supabase account identifier (a UUID)"). The auditor's WR-04 flag was based on the stale pre-fix REVIEW.md.
- Cross-reference: privacy invariant independently verified in `114-VERIFICATION.md` (6/6 SC) and `114-REVIEW.md` (0 blockers).

---

## Sign-Off

- [x] All threats have a disposition (mitigate / accept / transfer)
- [x] Accepted risks documented in Accepted Risks Log
- [x] `threats_open: 0` confirmed
- [x] `status: verified` set in frontmatter

**Approval:** verified 2026-06-16
