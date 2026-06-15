---
status: partial
phase: 114-usage-analytics
source: [114-VERIFICATION.md]
started: "2026-06-15T20:18:17Z"
updated: "2026-06-15T20:18:17Z"
---

## Current Test

[awaiting human testing]

## Tests

### 1. Live PostHog event delivery
expected: Opt the desktop app in to telemetry, log in with a Supabase account, let the startup coordinator fire (~700ms after launch). In the EU PostHog project a `desktop_session_start` event appears with `distinct_id` equal to the user's Supabase UUID (not an int hash); props contain only `app_version`, `os_family`, `os_version`, `python_version`, `pyqt_version`, `ui_language`, `session_id` — never hostname/username/executable-path/cwd; and the same `distinct_id` merges with the web session for that user into one person profile.
result: [pending]

### 2. Consent disclosure accuracy (WR-04)
expected: Decide on `desktop/consent_dialog.py` lines 306-308 (EN) and 339-340 (HE), which currently say "bare Supabase `user.id`" while the code sends `user._uuid` (the raw Supabase UUID string). Either reword to "Supabase account identifier (UUID)" or accept the existing wording with a documented rationale. The behavior is privacy-correct; only the disclosure text is potentially misleading.
result: [pending]

## Summary

total: 2
passed: 0
issues: 0
pending: 2
skipped: 0
blocked: 0

## Gaps
