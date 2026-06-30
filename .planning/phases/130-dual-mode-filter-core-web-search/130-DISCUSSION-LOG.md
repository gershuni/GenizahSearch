# Phase 130: Dual-Mode Filter Core — Web `/search` - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-30
**Phase:** 130-dual-mode-filter-core-web-search
**Areas discussed:** Dialog library universe, Mode switch + selection behavior, Default mode + first-open state, Button/label wording

---

## Dialog library universe (per mode)

| Option | Description | Selected |
|--------|-------------|----------|
| Hide = full list; Show-only = current facets | Mode-dependent universe | |
| Both modes: full canonical list | Always all ~15, annotated with counts | |
| Both modes: current-result facets only | Simplest; can't pre-hide | |

**User's choice:** Free-text refinement — **both modes** use a unified dialog: a shortlist of libraries SEEN in the current results (sorted by result count desc) on top, an expandable section with ALL OTHER libraries (sorted A–Z), and a text-search box to filter the long list quickly.
**Notes:** This beat all three offered options — it keeps the "what's here + counts" affordance while making every canonical library reachable (so a library can be pre-hidden before it ever appears in results), and the search box keeps the long list usable. Captured as D-01/D-02.

---

## Mode switch + selection behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Reset selection on switch | Each mode starts fresh; segmented toggle at top | ✓ |
| Keep selection on switch | Carries over, inverting meaning | |

**User's choice:** Reset selection on switch (Recommended).
**Notes:** Prevents silently inverting intent (e.g. "show only CUL" → "hide CUL"). Captured as D-03/D-04.

---

## Default mode + first-open state

| Option | Description | Selected |
|--------|-------------|----------|
| Show-only, all checked = show all | Matches today's behavior | |
| Hide, empty = show all | Opens in Hide mode, nothing hidden | ✓ |

**User's choice:** Hide, empty = show all.
**Notes:** Deliberate — the "hide a noisy library" exclude use-case is the primary motivation for the milestone, so a fresh user lands in Hide mode. Migrated legacy allowlists still open in Show-only (DMF-05). Captured as D-05/D-06.

---

## Button/label wording + chips

| Option | Description | Selected |
|--------|-------------|----------|
| Button-only, 3 states, bilingual | Neutral / "Showing N/total" / "Hiding N"; no chips | ✓ |
| Button + removable chips | Reverses v8.3.0 chips-removal | |

**User's choice:** Button-only, 3 states, bilingual (Recommended).
**Notes:** Consistent with the v8.3.0 smoke decision that removed chips and put state on the button. Captured as D-07.

## Claude's Discretion

- NiceGUI widget choices for the segmented toggle / expand / search affordances; internal persistence key/shape (subject to D-09 migration constraint); the `_apply_library_filter` mode branch (∈ for Show-only, ∉ for Hide).

## Deferred Ideas

- Desktop catalog parity, web Browse-by-Identification dual-mode, web `/parallels` control → Phase 131.
- Public API `mode` (include/exclude) → Phase 132.
- Cross-device sync of the preference → Future Requirements.
