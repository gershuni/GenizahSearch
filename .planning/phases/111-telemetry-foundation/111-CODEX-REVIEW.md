# Phase 111 — Codex External Code Review

**Reviewer:** Codex CLI (gpt-5.5, reasoning effort xhigh), full-access run
**Date:** 2026-06-14
**Scope:** `shared/posthog_server.py` (diff 6b5256df..HEAD) + `desktop/telemetry.py` (new, 744 lines)
**Verdict:** **FIX-FIRST** — consent-bypass seam + scrubber gaps violate stated privacy invariants
**Cross-check:** Claude verified every load-bearing premise against the code (notes inline). All
confirmed. These are gaps in the *threat model itself* (unmodeled vectors), which is why the
`111-SECURITY.md` audit — scoped to verifying *declared* mitigations — marked 20/20 closed yet
missed them.

> **Exploitability today:** mostly LATENT. The embedded key is still a placeholder (WR-05 nullifies
> it → no POST without `GENIZAH_TELEMETRY_KEY` env), and `track_error`/`identify`/`context` have no
> production callers yet (Phases 112-115 add them). These are foundation-phase design issues to fix
> *before* the real key lands (Phase 114) and before callers are wired.

---

## Findings

### F1 — BLOCKER → (Claude: HIGH, must-fix-before-Phase-114) | `desktop/telemetry.py:672`
**Import-time key wiring + ungated shared transport = consent-independent egress.**
`_wire_transport_config()` runs at import (line 672, before `_load_consent_state` at 673) and calls
`set_capture_api_key()` *unconditionally* — no consent check. The shared transport
(`shared/posthog_server.py`) is intentionally **ungated** (T-111-03), and other emitters call it
directly: **`shared/nli_circuit_breaker.py:185,202`** emit `nli_breaker_opened/closed` via
`enqueue_event` with no consent gate. In the desktop process these are reachable through
`genizah_core` NLI fetches. So once a real capture key is wired into the process transport, NLI
breaker events POST **without opt-in**.
**Claude verification:** Confirmed. *Not live today* — WR-05 (line 294-295) nullifies the placeholder
key, and a desktop has no `POSTHOG_API_KEY` env fallback. Becomes live by default when the real
`phc_` key is embedded in Phase 114 unless the architecture changes.
**Fix:** Gate the desktop's use of the shared transport on consent — only wire `set_capture_api_key`
*after* confirmed opt-in and clear it on opt-out (move the import-time `_wire_transport_config()`
call out of module scope), OR add a desktop-side consent gate in the shared transport for
non-`system`/desktop events. Keep web behavior unchanged.

### F2 — BLOCKER → (Claude: HIGH) | `desktop/telemetry.py:142` (`_PATH_RE`)
**Path scrubber misses common private-path forms.** `_PATH_RE = [A-Za-z]:\\\S+|/\S{3,}|\S+\.[A-Za-z]\w{0,7}\b`.
`\S+` stops at whitespace and there is no UNC branch. Codex ran the actual regex:
- `C:\Users\Jane Doe\Research Notes` → `[REDACTED] Doe\Research Notes` (username folder survives)
- `\\server\share\Jane Doe\notes` → **survives entirely** (no drive letter, no `/`, no extension)
**Claude verification:** Confirmed empirically (Codex executed the compiled regex). Defense-in-depth
layer; impact bounded by what flows through (today only `context` is free-text, no callers yet).
**Fix:** Replace with explicit drive-letter + UNC (`\\\\…`) + POSIX patterns that tolerate spaces,
or reject any free-form path-bearing value outright.

### F3 — HIGH → (Claude: MEDIUM, by-design but un-hardened) | `desktop/telemetry.py:582`
**`identify()` sends raw `user_id` as `distinct_id` outside the scrubber.** `identify()` scrubs only
the *properties*; `user_id` is passed straight to `enqueue_event(..., distinct_id=user_id)`, stored
as current identity, and persisted. A caller passing an email/name/path leaks it.
**Claude verification:** Confirmed (line 582 passes `user_id` unscrubbed). By design the caller is
desktop code passing the Supabase `user.id` (opaque UUID), so low practical risk — but unvalidated.
**Fix:** Validate `user_id` against an opaque-ID format, or hash/derive a pseudonymous distinct_id
before enqueue + persist.

### F4 — HIGH (Claude: concur, key design finding for the foundation) | `desktop/telemetry.py:545`
**Allowlisted `context` is a free-text escape hatch.** `track_error(context, exc)` stores
caller-controlled `context` under an allowlisted key. `_scrub_value` only redacts paths, bare
filenames, Hebrew, and over-length — so English / Arabic-script / transliterated private text
survives (Codex: `"Maimonides rent letter"` passes through unredacted).
**Claude verification:** Confirmed. The allowlist guards *keys*, not *values*; `context` defeats it.
Latent (no callers in Phase 111) but the whole point of a foundation phase is to lock this down
before Phases 112-115 start passing real context strings.
**Fix:** Make `context` a fixed enum / static code registry (no free text), or bucket/redact
arbitrary context values before `_emit()`.

### F5 — HIGH → (Claude: MEDIUM/HIGH) | `desktop/telemetry.py:402`
**Opt-out is not fail-closed.** `set_consent(False)` calls `save_app_config()` (line 402, disk I/O)
*before* flipping `_enabled=False` (line 404) and before `_drain_and_discard()` (line 411). A
concurrent `track()` in that window still passes `is_enabled()==True` and enqueues; the daemon can
POST it before the drain. Separately, `save_app_config()` swallows write failures inside the outer
`try/except`, so a failed opt-out leaves both memory and disk **enabled** for the next launch.
**Claude verification:** Confirmed by code ordering. On opt-out, flip in-memory consent + clear the
transport key FIRST, then drain, then persist.
**Fix:** Reorder: `_enabled=False` + `set_default_distinct_id(None)` + clear key → `_drain_and_discard()`
→ persist. Make persistence failure observable; fail closed on next launch if opt-out can't be confirmed.

### F6 — MEDIUM → (Claude: LOW/MEDIUM) | `shared/posthog_server.py:184`
**Process-wide default distinct_id conflates operational telemetry with user identity.** The transport
rewrites any `distinct_id=='system'` event to `_default_distinct_id` (line 184-187). NLI breaker
events omit `distinct_id` (→ `'system'`), so once the desktop sets the default (post opt-in), shared
operational events become user-linked — different semantics from the prior `system` events.
**Claude verification:** Confirmed. Only applies post-opt-in (consent already granted), so minor; but
combined with F1 these operational events shouldn't carry the user's install_id at all.
**Fix:** Don't substitute the default in the generic transport; have `desktop.telemetry` pass explicit
distinct_ids only for sanctioned desktop events.

---

## Could-not-verify (Codex)
- No live PostHog/network sends performed.
- The embedded key is a placeholder → F1 is live today only via `GENIZAH_TELEMETRY_KEY`; live by
  default once the real key is inserted (Phase 114).
- Phase 112-115 producers not reviewed beyond current committed callsites.

## Relationship to 111-SECURITY.md
The security audit verified that every *declared* plan-time mitigation exists in code (true — 20/20).
Codex found vectors the threat model never enumerated: the desktop↔shared-transport consent seam
(F1/F6), the scrubber's key-vs-value blind spot (F4) and regex gaps (F2), and opt-out ordering (F5).

---

## Resolution (2026-06-14)

**Fixed: F1, F2, F4, F5** (user-directed targeted pass). **Deferred: F3, F6.**
All changes are in `desktop/telemetry.py` only (no `shared/posthog_server.py` change needed).
Regression tests: `tests/test_telemetry_codex_review_fixes.py` (11 tests). Full Phase 111
telemetry suite **89 passed**, ruff clean.

| # | Status | Fix |
|---|--------|-----|
| **F1** | ✅ Fixed | Removed the unconditional import-time `_wire_transport_config()`. The capture key is now wired ONLY on a consented launch (`_load_consent_state` when `enabled`) and on `set_consent(True)`, and is **revoked** (`set_capture_api_key(None)`) on `set_consent(False)`. The ungated shared transport (NLI breaker) therefore has no key — and cannot POST — unless the user has currently consented. Tests: `test_key_not_wired_on_unconsented_launch`, `test_key_wired_on_consented_launch`, `test_opt_out_revokes_transport_key`. |
| **F2** | ✅ Fixed | Rewrote `_PATH_RE`: added a UNC branch (`\\server\share\…`) and a `_PATH_TAIL` that consumes single internal spaces so paths with spaces redact fully (no more leaked `Jane Doe` after the space). Empirically verified; prose/versions/`and/or` not over-redacted. Tests: `test_windows_path_with_spaces_fully_redacted`, `test_unc_path_redacted`, `test_posix_path_with_spaces_redacted`, `test_prose_not_over_redacted`. |
| **F4** | ✅ Fixed | Added `_safe_context()` — `context` must be an identifier-shaped code (`[A-Za-z0-9._-]+`, ≤64 chars); anything else (English/transliterated prose, Hebrew, paths, over-long) collapses to `'unregistered'`. Applied in `_emit` after `_scrub_props` (sourced from the pre-scrub value) so it covers `track_error` AND any `track(..., context=)` callsite without being mangled by the path redactor. Tests: `test_safe_context_collapses_free_text`, `test_safe_context_preserves_code_labels`, `test_track_error_context_collapsed_end_to_end`. |
| **F5** | ✅ Fixed | `set_consent(False)` now shuts the in-memory gate, clears the default distinct_id, revokes the key, and drains the queue **before** the disk write (closes the concurrent-`track()` race), then verifies the opt-out actually persisted (read-back) and logs a WARNING if not — no longer fails open silently. Tests: `test_opt_out_shuts_gate_before_persisting`, `test_opt_out_failed_persist_is_not_silent`. |
| **F3** | ⏳ Deferred | `identify()` user_id validation — by-design (Supabase opaque UUID); hardening tracked in `docs/OPEN_ISSUES.md`. |
| **F6** | ⏳ Deferred | default-distinct_id substitution conflating NLI-breaker operational events — minor, post-opt-in only; tracked in `docs/OPEN_ISSUES.md`. With F1 fixed, those events now only fire under consent. |
