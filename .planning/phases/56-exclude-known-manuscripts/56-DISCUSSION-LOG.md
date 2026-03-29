# Phase 56: Exclude Known Manuscripts - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md -- this log preserves the alternatives considered.

**Date:** 2026-03-29
**Phase:** 56-exclude-known-manuscripts
**Areas discussed:** Exclusion source picker, File import UX, Exclusion display in results, Interaction with refinement chain

---

## Exclusion Source Picker

| Option | Description | Selected |
|--------|-------------|----------|
| Button in filter panel | "Exclude manuscripts" alongside domain/material/measurement filters, opens dialog | |
| Post-search action | Button near results count (like "Search within N") | |
| Both | Filter panel button + post-search button, both apps | ✓ |

**User's choice:** Both (also desktop)
**Notes:** User emphasized both apps (web + desktop) must have this.

---

## File Import UX

### Formats

| Option | Description | Selected |
|--------|-------------|----------|
| Text only | One shelfmark per line, simplest | |
| Text + CSV | Text file + CSV with auto-detect shelfmark column | ✓ |
| Text + CSV + Excel | Broadest compat but adds dependency | |

**User's choice:** Text + CSV

### Resolution Report

| Option | Description | Selected |
|--------|-------------|----------|
| Inline summary | "Resolved 80/100. 20 not found." with expandable failures | |
| Dialog with table | Full table showing each shelfmark -> resolved sys_id or "not found" | ✓ |

**User's choice:** Table following desktop composition search pattern (shows sys_id for found items). User was unsure initially, referenced the existing desktop composition search table as a good model.

---

## Exclusion Display in Results

| Option | Description | Selected |
|--------|-------------|----------|
| Count only | Just "3 excluded from 'My reviewed list'" in header, no expanded view | |
| Collapsible excluded section | Count + expandable section at bottom showing excluded manuscripts with source | ✓ |
| Count badge + per-source chips | Chips near results count for each source with clear buttons | |

**User's choice:** Option 2 (collapsible section), though noted option 1 is also acceptable.

---

## Interaction with Refinement Chain

| Option | Description | Selected |
|--------|-------------|----------|
| Independent post-filter | Separate from refinement chain, applied after search | ✓ |
| Merged into breadcrumb | Different-colored chips in refinement strip | |
| Pre-filter via restrict_sys_ids | Exclusions feed into Tantivy restriction | |

**User's choice:** Post-search filter. Rationale: "we don't exclude much even with large lists" -- no performance benefit from pre-filtering.

---

## Claude's Discretion

- Dialog layout (tabs vs sections)
- Resolution report table columns
- Button placement details
- Desktop QDialog design

## Deferred Ideas

None
