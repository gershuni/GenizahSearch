---
status: investigating
trigger: "app-storage-user-assertion-sweep: systematic sweep of all unguarded app.storage.user accesses"
created: 2026-03-27T00:00:00Z
updated: 2026-03-27T00:00:00Z
---

## Current Focus

hypothesis: Multiple unguarded app.storage.user accesses exist in page-construction code paths across web/
test: grep all accesses, classify as safe (event handler/callback) vs vulnerable (page construction)
expecting: find remaining vulnerable accesses beyond the 3 already fixed
next_action: grep all app.storage.user accesses

## Symptoms

expected: All pages load normally even on first request after service restart
actual: 500 errors with AssertionError in nicegui/storage.py line 115
errors: AssertionError: user storage for {uuid} should be created before accessing it
reproduction: Restart web service, hit any page — first request may crash
started: Ongoing, surfaced during live debugging

## Eliminated

## Evidence

## Resolution

root_cause: app.storage.user property raises AssertionError when accessed before NiceGUI session init
fix:
verification:
files_changed: []
