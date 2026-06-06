---
status: partial
phase: 108-desktop-join-workbench-query-builders-candidates-compare
source: [108-VERIFICATION.md, 108-REDESIGN-SUMMARY.md]
started: 2026-06-05T12:59:01Z
updated: 2026-06-06T00:00:00Z
---

## Current Test

[awaiting human testing of the REDESIGNED Join Lab]

## Tests

### 1. Layout & anchor pane
expected: Join Lab opens. Anchor pane defaults to ~30% width and is resizable (drag the splitter). The four anchor actions are ICON-ONLY (📖 browse / 🧩 puzzle / ☰ list / 🔗 join, no text) at the bottom of the anchor pane and act on the ANCHOR.
result: [pending]

### 2. Query builder — per-line ⚙, gap, signs, Search options
expected: Each query line has a ⚙ that opens THAT line's options (negation/plene/prefix/suffix/wildcards + ⊢ starts / ⊣ ends line); wildcard-prefix is greyed when the line has >1 OR box. The gap "↓N" is a compact inline control on the line (no extra row). The ⓘ tooltip lists the sign legend, and typing #/word#/%/*word/word*/− in a box works. A "Search options ▾" button holds the global toggles (variants / Judeo-Arabic / flex / bidirectional). The read-only preview updates.
result: [pending]

### 3. Grid candidates + context menu
expected: Grid shows ~20/page; each card has a selection checkbox, a thumbnail, material/dims, a highlighted snippet, Y/?/N triage, and ICON-ONLY action buttons (📖🧩☰🔗 + ⇄ compare) with tooltips. Right-click a card → context menu with the same actions + triage.
result: [pending]

### 4. Table checkboxes + shared bulk bar
expected: Table view has a leading checkbox column; clicking the column-0 header toggles all. Checking items (grid OR table) shows a shared bulk-action bar; 📖 Browse and 🔗 Add-as-join are enabled ONLY when exactly one is checked; 🧩 Add-to-Puzzle and ☰ Add-to-list act on all checked. Selection survives grid paging and grid↔table toggle.
result: [pending]

### 5. Browse results / Compare
expected: "Browse results ▶" (next to Grid/Table) opens the side-by-side compare stepping the filtered list (starts at the single selected item if exactly one, else the first); card ⇄ and table double-click also open compare. Cross-side candidates open to the neighbor page with an "other side matched" label; all four actions + Y/?/N triage + Re-anchor are reachable inside; the anchor pane stays static across Prev/Next.
result: [pending]

### 6. Puzzle includes the anchor
expected: "Add to Puzzle" from a grid card, from the bulk bar, and from inside Compare adds the ANCHOR plus the candidate(s) to the puzzle canvas (anchor first, de-duped).
result: [pending]

### 7. Filter dialog with current-fragment info
expected: "Filter ▾" opens a dialog with text/material/has-dimensions/triage/size controls, PLUS a "Current fragment" panel showing the anchor's known fields (library/material/size/lines — unknowns omitted) and "from anchor" shortcuts. Filters narrow the candidate list.
result: [pending]

### 8. Other side + global options reach the engine
expected: "search also on the other side of the leaf (p ±1)" with AND-narrow / OR-widen narrows/widens via the adjacent page. Toggling Judeo-Arabic / Flex Spacing / Bidirectional in Search options changes the result set (the globals reach the engine — RR-14).
result: [pending]

## Summary

total: 8
passed: 0
issues: 0
pending: 8
skipped: 0
blocked: 0

## Gaps
