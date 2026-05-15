# Phase 999.2: Filtering by PGP - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-15
**Phase:** 999.2-filtering-by-pgp
**Areas discussed:** Mode, Where, Visibility, Labels, Persistence

---

## Filter mode

| Option | Description | Selected |
|--------|-------------|----------|
| Post-search result-list toggle (3-state: all / only with PGP / hide PGP) | Mirror the existing 'Filter Printed' button at web/pages/search.py:1430. (Recommended) | ✓ |
| Pre-search filter in the filter panel | Search engine itself only returns matches from PGP-tagged manuscripts. Requires plumbing through filter_panel.py |  |
| Both: pre-search and post-search | Filter panel option + result-toolbar toggle |  |

**User's choice:** Post-search result-list toggle.
**Notes:** Confirms the printed_filter pattern is the reference implementation.

---

## Where it applies

Multi-select.

| Option | Description | Selected |
|--------|-------------|----------|
| Search results page | The main /search results bar, next to 'Filter Printed' button | ✓ |
| Parallels page | Same module (filter_panel) used by parallels |  |
| Desktop app | genizah_app.py search-results toolbar |  |

**User's choice:** "Search results page, It may be in the filter panel,"
**Notes:** Search results toolbar is the primary surface. "May be in the filter panel" is a soft signal — captured as Claude's discretion to revisit during planning. Parallels and desktop deliberately excluded → Deferred Ideas.

---

## Visibility gating

| Option | Description | Selected |
|--------|-------------|----------|
| Hidden until the current result set has at least one PGP-tagged hit | Matches the printed_filter pattern. (Recommended) | ✓ |
| Always visible after a search runs | Simpler logic but adds noise when no PGP data |  |
| Always visible (even before searching) | Always present in the toolbar |  |

**User's choice:** Hidden until at least one PGP-tagged hit exists.

---

## Button state labels

| Option | Description | Selected |
|--------|-------------|----------|
| 'Filter PGP' / 'Only PGP' / 'Hiding PGP' (matches printed_filter wording) | Exact parallel to existing button. (Recommended) |  |
| 'Has PGP' / 'No PGP' / 'All' | Shorter, chip-style | ✓ |
| Icon-only with tooltip | Smallest footprint |  |

**User's choice:** 'Has PGP' / 'No PGP' / 'All'.
**Notes:** User chose brevity. Wording locked exactly: `all` → 'All', `only_pgp` → 'Has PGP', `hide_pgp` → 'No PGP'.

---

## Persistence

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — via persist_value('search_pgp_filter', ...) | Same pattern as printed_filter at web/pages/search.py:1406. (Recommended) | ✓ (plus extra requirement) |
| No — reset to 'all' on every page load | Cleaner default each session |  |

**User's choice:** "Persists with clear badge in main results if there is a filter (i.e no PGP / only PGP)"
**Notes:** User added a requirement beyond persistence: when the filter is active (not 'all'), show a clear visible badge / chip in the main results area indicating the active filter state. Position near the existing `exclusion_chips_row` at search.py:1448. Captured as D-08 in CONTEXT.md.

---

## Claude's Discretion

- Whether to also expose the filter from inside `filter_panel.py` dialog (user softly signaled this is OK).
- Exact button icon and color tokens.
- Chip dismiss / clear-filter UX.

## Deferred Ideas

- Pre-search PGP filter (engine-level).
- Same toggle on the parallels page.
- Same toggle on the desktop app.
- Filter by PGP source / author / version (more granular).
