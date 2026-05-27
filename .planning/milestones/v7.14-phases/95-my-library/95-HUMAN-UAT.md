---
status: partial
phase: 95-my-library
source: [95-VERIFICATION.md]
started: 2026-05-21T17:00:00Z
updated: 2026-05-21T17:00:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. LOCAL filter button full UX cycle
expected: Filter button visible iff LOCAL hits exist. Cycles `Filter Local → Only Local → No Local → Filter Local` on Search / Composition / Parallels surfaces. State persists across app restart via session JSON. Each surface has independent state.
result: passed (covered by 95-08 smoke approved 2026-05-21)

### 2. LOCAL double-click → ResultDialog + Open File
expected: Double-clicking a LOCAL row in search results opens ResultDialog with the file text rendered + a blue "Open file" button that launches `os.startfile(filepath)`.
result: passed (covered by 95-08 smoke approved 2026-05-21)

### 3. D-10 P1 NO-OP chip
expected: With filter state `Only Local` and zero LOCAL hits in the current query, the filter renders as a NO-OP — Genizah results stay visible, and an inline chip `"My Library filter inactive — no LOCAL hits in this query"` appears.
result: passed (covered by 95-08 smoke approved 2026-05-21)

### 4. Above-ceiling warning dialog (D-26 / D-41 / W8)
expected: Add Folder with > 5,000 files OR > 2 GB total → per-folder ceiling dialog. Refresh across folders whose AGGREGATE totals exceed the threshold → aggregate ceiling dialog. Dialog title differentiates "Add — pre-scan" vs "Refresh — pre-scan".
result: [pending]

### 5. Seewald attribution in About dialog
expected: Web About page (`web/pages/about.py`) and desktop About dialog credit Yehuda Seewald's external prototype (`seewald_addition/`) as the inspiration for the My Library feature. Both EN and HE renderings present. D-31/D-32/D-33 cleartext disclosure language reads naturally.
result: [pending]

### 6. Help page My Library section
expected: Web Help (`/help` page) shows a "My Library" section with EN + HE content explaining: (a) what My Library does, (b) supported file types (`.docx` / `.pdf` / `.txt`), (c) that LOCAL data NEVER leaves the device, (d) how the 3-state filter + pre-search corpus dropdown interact, (e) Open File launches the source. Hebrew renders RTL correctly.
result: [pending]

## Summary

total: 6
passed: 3
issues: 0
pending: 3
skipped: 0
blocked: 0

## Gaps

[none — pending items are forward-looking checks, not regressions]
