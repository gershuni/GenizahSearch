---
status: diagnosed
phase: 26-scientific-joins
source: 26-01-SUMMARY.md
started: 2026-02-12T04:15:00Z
updated: 2026-02-12T04:30:00Z
---

## Current Test

[testing complete]

## Tests

### 1. FJMS Joins Visible in Web App
expected: Navigate to a manuscript with FJMS join data in the web app. The Related Fragments section shows FJMS join group members with a purple badge distinguishing them from PGP (blue) entries.
result: pass

### 2. Scholar Name and Join Type Display (Web)
expected: Each FJMS join entry in the web app shows the scholar's name (who identified the join) and the join type (e.g., Physical Join, Codex Join).
result: pass

### 3. FJMS Joins in Desktop App
expected: Open the same manuscript in the desktop app. The joins dropdown shows FJMS entries with an [FJMS] prefix label, visually distinct from PGP and user joins.
result: issue
reported: "In web, I see for 990001663820205171 only the entries without type info, and in desktop I see it duplicate, two without the type info and two with"
severity: major

### 4. Desktop JoinsDialog
expected: Click "Show All Joins" (or equivalent) in the desktop app. The JoinsDialog displays FJMS entries with scholar name and join type columns populated.
result: pass

### 5. Deduplication Across Sources
expected: If a fragment appears in both PGP and FJMS join data, only one entry shows (PGP takes priority). No duplicate fragments in the Related Fragments list.
result: pass

### 6. Navigation from FJMS Join Entry
expected: Click an FJMS join group member in the web app Related Fragments panel. It navigates to that fragment's browse page.
result: pass

## Summary

total: 6
passed: 5
issues: 1
pending: 0
skipped: 0

## Gaps

- truth: "When a manuscript belongs to multiple FJMS join groups, each partner fragment should appear only once with the richest metadata (join type preferred)"
  status: failed
  reason: "User reported: In web, only entries without type info shown (first-encountered wins dedup). In desktop, duplicates shown (two without type, two with type) for same partners across join groups."
  severity: major
  test: 3
  root_cause: "get_join_group() in shared/fjms_service.py returns raw duplicate rows when manuscript belongs to multiple join groups. SQL query selects ALL members from ALL groups without deduplication. Web dedup keeps first entry (losing richer metadata), desktop has no intra-FJMS dedup at all."
  artifacts:
    - path: "shared/fjms_service.py"
      issue: "get_join_group() SQL lacks GROUP BY or DISTINCT on AlmaId — returns duplicates across groups"
    - path: "web/components/joins_panel.py"
      issue: "Lines 171-218: first-encountered-wins dedup drops join_type from later groups"
    - path: "corrections_ui.py"
      issue: "_get_fjms_joins() (line 3490): no intra-FJMS dedup, all duplicates passed through"
  missing:
    - "Deduplicate in get_join_group() at service level: return each unique partner AlmaId once, preferring rows with non-NULL join_type"
    - "Add test coverage for multi-group membership deduplication"
  debug_session: ".planning/debug/fjms-join-deduplication.md"
