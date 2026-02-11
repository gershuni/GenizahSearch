---
status: complete
phase: 23-web-pending-corrections-display
source: 23-01-SUMMARY.md
started: 2026-02-11T16:00:00Z
updated: 2026-02-11T16:05:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Pending corrections appear in version selector
expected: Log in, browse to a manuscript page where you have a pending correction. Open the version selector menu. Your pending correction(s) should appear as entries in the list.
result: pass
note: User suggested defaulting to user's own edit if one exists, instead of version 0.8

### 2. Pending corrections have distinct amber styling
expected: In the version selector, pending corrections should be visually distinct from approved ones — amber/orange color scheme, a schedule/clock icon, and a status label indicating the pending state.
result: pass

### 3. Selecting a pending correction displays its text
expected: Click on a pending correction in the version selector. The corrected text should display in the reading area, same as selecting any other version.
result: pass

### 4. No pending corrections when logged out
expected: Log out (or open in incognito). Browse to the same manuscript page. The version selector should NOT show any pending corrections — it should look exactly as it did before this feature.
result: pass

### 5. No pending corrections when none exist
expected: While logged in, browse to a manuscript page where you have NO pending corrections. The version selector should behave exactly as before — no extra entries, no amber section.
result: pass

## Summary

total: 5
passed: 5
issues: 0
pending: 0
skipped: 0

## Gaps

[none yet]
