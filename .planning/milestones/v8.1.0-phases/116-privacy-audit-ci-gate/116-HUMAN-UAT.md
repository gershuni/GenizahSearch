---
status: partial
phase: 116-privacy-audit-ci-gate
source: [116-VERIFICATION.md, 116-02-PLAN.md Task 3]
started: 2026-06-16T12:00:00Z
updated: 2026-06-16T14:05:00Z
---

## Current Test

[Frozen-exe live delivery + privacy VERIFIED via production PostHog 134161 (2026-06-16 session).
Strict gold-standard items remaining: clean no-Python VM SSL_OK token + offline-degradation arm.]

## Tests

### 1. Frozen-exe live delivery + on-the-wire privacy (real-session evidence)
expected: |
  A real frozen-GenizahSearchPro.exe session delivers desktop_* events to PostHog 134161 over
  TLS, and the delivered events contain NO forbidden content (no query/search text, My-Library
  paths, filenames, usernames, hostnames, or raw tracebacks).
result: PASS (2026-06-16). Verified by querying production PostHog 134161 directly. The
  2026-06-16 16:56–17:00 IDT session (frozen exe, app_version 8.0.0) delivered
  desktop_session_start / desktop_search_executed / desktop_tab_activated / desktop_feature_opened
  / desktop_indexing_complete / desktop_session_performance_summary. Delivery over TLS from the
  frozen binary works (this closes the Phase 114 live-delivery UAT). Property audit of the REAL
  events: search_executed carried only search_mode/corpus_scope/result_count_bucket/action (NO
  query text); indexing_complete carried operation_kind/doc_count_bucket/duration_ms (NO
  filenames/paths); crash carried exc_type/lineno/module/fingerprint (NO traceback/paths);
  tab/feature carried enum names only. Identity-alignment confirmed (logged-out events tagged
  $process_person_profile=false; logged-in uses a person-profile UUID). PRIV-04 holds in
  production on real data.
  NOTE (not a defect): PostHog server-side GeoIP enrichment attaches city-level location
  ($geoip_city_name "Tel Aviv", lat/lon) derived from the source IP — NOT sent by the desktop
  payload, identical to the web app's behavior on the shared project. Flagged for the privacy
  disclosure ("anonymous") awareness; consistent with the shared-project posture.

### 2. SC#3 strict gold-standard — clean no-Python VM SSL_OK token + offline degradation
expected: |
  On a CLEAN Windows VM with NO Python installed:
  1. Network UP — `.\GenizahSearchPro.exe --telemetry-selftest` → stdout `SSL_OK`, exit 0
     (proves certifi cacert.pem is bundled INSIDE the frozen binary, not borrowed from a
     dev-machine Python). SSL_FAIL / NO_KEY = release blocker.
  2. Disable the network adapter — `.\GenizahSearchPro.exe --telemetry-selftest-offline`
     → `OFFLINE_OK` fast (< ~2s, no network call).
  3. With the adapter still disabled, launch normally → usable + silent (INFRA-05 offline proof).
result: [pending — recommended at /release on a clean VM. Test 1 already gives strong PRACTICAL
  evidence the frozen exe's bundled certifi works (real events delivered over TLS), but the
  clean no-Python VM is the strict proof that rules out any borrowed-SSL/env contamination, and
  the OFFLINE_OK / silent-offline arm has not been exercised yet.]

## Summary

total: 2
passed: 1
issues: 0
pending: 1
skipped: 0
blocked: 0

## Gaps

(No defects found. Test 1 (live delivery + privacy) PASSED on real production data. Test 2 (the
strict clean-VM SSL_OK token + offline arm) is the only remaining item and is recommended at
/release. PRIV-04 and INFRA-06 completion flips in REQUIREMENTS.md remain deferred to the
milestone verification pass per the deferral note in 116-VERIFICATION.md — Test 1 satisfies the
delivery + privacy components; the clean-VM certifi proof (Test 2) is the last strict check.)
