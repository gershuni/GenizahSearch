# Phase 107: Desktop Join Workbench — Anchor, Entry Points, Actions & Join Model - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-04
**Phase:** 107-desktop-join-workbench-anchor-entry-points-actions-join-model
**Areas discussed:** Workbench shell / host, Known-joins group display, Add-as-Join before candidates, Anchor nav & re-anchor

---

## Workbench shell / host

| Option | Description | Selected |
|--------|-------------|----------|
| Dedicated modeless window | Floating window opened FOR an anchor (sketch placeholder, UAT'd) | ✓ |
| New 8th main-window tab | Persistent tab alongside the existing 7 | |
| Dock panel / Join Mode overlay | Dockable side panel or mode toggle | |

**User's choice:** Dedicated modeless window.
**Notes:** Matches the entry model ("Find joins" FOR this fragment / cold-start by shelfmark) and the validated sketch.

| Option (lifecycle) | Description | Selected |
|--------|-------------|----------|
| Re-anchor the existing one | Reuse the single open window, swap anchor | ✓ |
| Open a second workbench | Independent window per invocation | |
| Ask / focus existing | Prompt re-anchor vs new | |

**User's choice:** Re-anchor the existing one (single `self._join_workbench` ref).

---

## Known-joins group display

| Option | Description | Selected |
|--------|-------------|----------|
| Flat list + per-row source badge | One list, badge PGP/FJMS/user/community | ✓ |
| Grouped sections by source | Separate labelled sections | |
| Plain list, no source | Connected shelfmarks only | |

**User's choice:** Flat list + per-row source badge. **Notes:** Degrades to a generic "known join" tag if the BFS closure doesn't expose per-member provenance (research flag R-01).

| Option (row content) | Description | Selected |
|--------|-------------|----------|
| Shelfmark + title + thumbnail | Small image + shelfmark + title (batched) | ✓ |
| Shelfmark + title (text only) | No thumbnail | |
| Shelfmark only | Minimal one-line | |

**User's choice:** Shelfmark + title + thumbnail.

| Option (empty state) | Description | Selected |
|--------|-------------|----------|
| Friendly prompt + Add-as-Join affordance | Turns empty state into start of workflow | |
| Plain 'none' line | Simple "— no known joins —" | |
| **Nothing shows** (free-text) | Panel hidden entirely when no known joins | ✓ |

**User's choice:** Nothing shows — known-joins panel is hidden entirely when empty. **Consequence:** Add-as-Join cannot live inside the panel; it sits on the anchor action-row.

---

## Add-as-Join before candidates

| Option | Description | Selected |
|--------|-------------|----------|
| Existing JoinsDialog, anchor pre-filled | Open JoinsDialog (anchor=A); scholar enters B | ✓ |
| By-shelfmark partner picker in workbench | Inline shelfmark field → persist API directly | |
| Wire persist API + refresh only; defer add-UI to 108 | Prove the action against a known partner | |

**User's choice:** Existing JoinsDialog pre-filled. **Notes:** Research flag R-02 — confirm free partner-B entry without a pre-supplied candidate.

| Option (action home) | Description | Selected |
|--------|-------------|----------|
| Anchor action-row + per known-join row | Actions on anchor + on each known-join member | ✓ |
| Anchor action-row only | Known-join rows display-only | |
| A single workbench toolbar | One toolbar, current selection | |

**User's choice:** Anchor action-row + per known-join row. **Notes:** Add-as-Join on the anchor row so it works even when the joins panel is hidden.

---

## Anchor nav & re-anchor

| Option (folio nav) | Description | Selected |
|--------|-------------|----------|
| Views pages of same fragment; anchor (sys_id) unchanged | Viewer-only; group not reloaded per page | ✓ |
| Each folio is its own anchor | Re-anchor per page | |

**User's choice:** Views pages of the same fragment; anchor sys_id unchanged. **Notes:** Current page recorded for 108's "other side = p±1"; viewer-only in 107.

| Option (re-anchor) | Description | Selected |
|--------|-------------|----------|
| Yes — explicit 're-anchor' per known-join row | "⚓ make anchor" reloads that fragment's group | ✓ |
| No — known-join rows only Browse | Re-anchor stays a 108 concern | |

**User's choice:** Yes — explicit "⚓ make anchor" per known-join row (walk the connected component); explicit action, not single-click.

## Claude's Discretion

- Cold-start ambiguous-shelfmark resolution UX (reuse `resolve_system_by_shelfmark` options).
- No-image fallback ("(no image)" placeholder).
- Exact zoom step, metadata-line composition, window sizing/title, close/cleanup.
- `QDialog` vs `QMainWindow`; whether the optional result-row 🔗 trigger is retained.

## Deferred Ideas

- Candidate search / builders / Compare → Phase 108. VS source / combined / soft-retire → Phase 109.
  JSA → Phase 110. Web UI → later. Multiple windows, dock/overlay host, richer N-fragment join model,
  multi-leaf adjacency → deferred. 4 keyword-coincidence todos reviewed, not folded.
