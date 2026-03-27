# Phase 54: Dimensions Filtering - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-03-27
**Phase:** 54-dimensions-display-filtering (plan 54-03: filtering)
**Areas discussed:** Filter fields, Input UX, Placement, Pre/post-search behavior, Apply mode, Review findings

---

## Filter Fields

| Option | Description | Selected |
|--------|-------------|----------|
| Width & height only | Simple, covers main use case | |
| Width, height & line count | Adds line count range | |
| Width, height, line count & material | Full set | |
| All + line height/density | All above plus line height and text density for join matching | ✓ |

**User's choice:** All fields including line height and text density
**Notes:** User emphasized that line height and text density are MORE important than page dimensions for join matching — fragments of the same manuscript share scribal characteristics but not physical dimensions. This is the key insight driving field selection.

---

## Input UX

| Option | Description | Selected |
|--------|-------------|----------|
| Min/max number inputs | Two small text boxes per field, leave blank = no constraint | ✓ |
| Range sliders with number inputs | Dual-handle slider + editable number boxes | |
| Preset ranges + custom | Quick-pick buttons plus custom min/max | |

**User's choice:** Min/max number inputs (recommended)

---

## Placement

| Option | Description | Selected |
|--------|-------------|----------|
| In existing filter panel | Add below existing filters, collapsible section | ✓ |
| Separate 'Measurements' section | Distinct collapsible group | |
| Advanced search tab/mode | Behind extra click | |

**User's choice:** In existing filter panel (recommended)

---

## Pre-search vs Post-search

| Option | Description | Selected |
|--------|-------------|----------|
| Same filters, both locations | Identical inputs pre-search and post-search | ✓ |
| Pre-search only | Dimension filters only in pre-search panel | |
| Pre-search with post-search summary | Full inputs pre-search, stats-only post-search | |

**User's choice:** Same filters, both locations (recommended)

---

## Apply Mode (Post-search)

| Option | Description | Selected |
|--------|-------------|----------|
| Live/debounced | Results narrow as user types, ~500ms debounce | |
| Apply button | User sets ranges then clicks Apply | ✓ |

**User's choice:** Apply button

---

## Review Findings Response

| Finding | Decision | Selected |
|---------|----------|----------|
| Accept unpaired aggregation approximation | Summary table is fast enough, false positives are rare | ✓ |
| Address paired observations | Scan detail rows for accuracy | |

**User's choice:** Accept approximation (recommended)

---

## Claude's Discretion

- Schema details for avg_line_height_mm column addition
- Desktop filter dialog layout
- Filter chip display format
- Exact aggregation formulas

## Deferred Ideas

- "Include manuscripts with unknown measurements" toggle
- "N results would match" live preview for iterative range exploration
