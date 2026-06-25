# Phase 120: Actions & Persistence - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-20
**Phase:** 120-actions-persistence
**Areas discussed:** Add-as-Join target, Phase scope (SEED-007), Export shape (ACT-03), Restore behavior (PST-01), Lists↔Lab integration (user-added)

---

## Add-as-Join target (ACT-01)

| Option | Description | Selected |
|--------|-------------|----------|
| Supabase community join | Write via existing `create_fragment_join` (RLS per-user, login-gated); flows into the known-joins group. joins.db role covered by Add-to-Puzzle. | ✓ |
| joins.db saved-join document | Desktop parity / SEED-007 #4. But web joins.db is a SHARED sidecar — multitenant-unsafe for per-user proposals. | |
| Both paths | Community join AND joins.db document. Redundant + inherits shared-db concern. | |

**User's choice:** Supabase community join.
**Notes:** Resolved the apparent ROADMAP↔SEED conflict — on web the joins.db "saved document" role IS the Fragment Puzzle (ACT-02), so the two actions are complementary, not competing.

### Sub-question — added-join visibility

| Option | Description | Selected |
|--------|-------------|----------|
| Optimistic, creator-only | Append to creator's session group; global cache stays confirmed-only. | |
| Submitted-pending, not shown | Save pending; toast; not shown until confirmed. | |
| Status-aware group fetch | Make group fetch user+status-aware. | |

**User's choice:** Free-text — *"I think that users' joins are auto-confirmed. Check it."*
**Notes:** Investigated: committed code says `status DEFAULT 'proposed'` + group filters `confirmed`, which would HIDE new joins. User then verified LIVE on production that a regular user's join shows immediately to everyone, and that there is NO confirm UI and NO delete UI anywhere. → ACT-01 matches live publish-to-everyone behavior (researcher to verify the live status default and set status explicitly); reconciles with 118 D-17.

### Sub-question — delete/undo a join

| Option | Description | Selected |
|--------|-------------|----------|
| Add "remove my join" (self) | Wire existing `delete_fragment_join` + delete-own RLS for the creator's own joins. | ✓ |
| Add-only, defer delete | Keep creating only; note delete as follow-up. | |
| Self + admin delete | Self-delete plus an admin-any delete (bigger surface). | |

**User's choice:** Add "remove my join" (self-scoped).
**Notes:** Motivated by the live finding that no delete UI exists at all; the Lab makes creation easier so accidental joins are likelier.

---

## Phase scope (SEED-007)

| Option | Description | Selected |
|--------|-------------|----------|
| Make-an-anchor (#2) | Promote a candidate/fragment to the anchor slot. | ✓ |
| Browse-in-Compare (#5) | Open candidate/anchor in /browse from Compare. | ✓ |
| Compare info buttons (#6) | FJMS catalog + bib metadata per pane in Compare. | ✓ |
| Show anchor's saved joins.db joins (#3) | Reverse-lookup saved join-docs containing the anchor (shared-db noise caveat). | |

**User's choice:** Make-an-anchor, Browse-in-Compare, Compare info buttons + a NEW item (via Other): *"Silently loading images in bg when navigating in Compare Window, allowing for fast navigation"* (Compare image prefetch). Deselected #3.
**Notes:** Prefetch scoped as off-loop adjacent-candidate preload, bounded pool, SEED-008-guarded.

### Sub-question — SEED-007 #1 (stop search, keep partial results)

| Option | Description | Selected |
|--------|-------------|----------|
| Visible Stop button only | Cancel; keep prior on-screen results (no partials). | |
| True partial results | Engine-streaming changes to keep mid-flight partials. | |
| Defer entirely | Drop #1 this phase. | |

**User's choice:** Free-text — *"Currently the web search allows for stop and show partial results. Isn't it the same search behavior?"* + a 2nd item: *"don't show Visual Similarity switch for fragments without VS."*
**Notes:** User was right — `execute_search` already returns partials on cancel (InterruptedError) and `/search` exposes a Stop-shows-partials button; the Lab merely discards them via the supersession guard. → #1 IN as a visible Stop that applies partials on EXPLICIT stop (parity with /search), keep discarding superseded-run partials. The VS-switch-hide item added as a scope item (D-12).

---

## Export shape (ACT-03)

| Option | Description | Selected |
|--------|-------------|----------|
| Flat table, CSV + XLSX | Single table incl. shelfmark/library/title/material/dims/triage/image URL. | ✓ |
| Full 4-sheet research XLSX | Reuse export_dossier dossier with triage grafted in. | |
| Both, user picks at export | Offer flat or dossier at export time. | |

**User's choice:** Flat table (CSV + XLSX) — *"but it has to include text of one page (the searched term page, or in vs the first text page)."*
**Notes:** Add a transcription-text column: matched page for text hits, first text page for VS-only. Off-loop/batched/text-capped; anonymous (Add-to-List stays login-gated).

---

## Restore behavior (PST-01)

| Option | Description | Selected |
|--------|-------------|----------|
| Auto re-run + "restoring…" indicator | Auto re-run from persisted inputs on return; repopulate grid. | ✓ |
| Auto for light modes, button for fuzzy | Auto for exact/variants; manual re-run for the up-to-300s fuzzy tier. | |
| Always one-tap re-run | Restore inputs instantly, never auto-run. | |

**User's choice:** Auto re-run + "restoring…" indicator.
**Notes:** Triage/filter/view re-attach by sys_id; never persist result/full_text/image blobs.

---

## Lists ↔ Joins Lab integration (user-added, "explore more")

**User's free-text:** *"We didn't wire Choose Anchor from list (also the 'sign in' there goes to settings instead to sign in). Also we have to add in the lists page a button for Open in Joins Lab (link icon between Browse and Puzzle)."*

| Item | Resolution |
|------|------------|
| Choose-anchor-from-list authenticated picker | Wire it (currently the logged-in dialog only links to /lists; `joins_lab.py:1561`) — D-17. |
| Sign-in route bug | `joins_lab.py:1573` navigates to `/settings` instead of the real sign-in flow — fix to corrections.py login flow — D-18. |
| "Open in Joins Lab" on /lists | New `link`-icon button between Browse and Puzzle in the list-item row → `/joins-lab?sys_id=` — D-19. |

### Add-to-Puzzle / Add-to-List selection source

**User's choice:** "Write CONTEXT.md" (accepted the default) — bulk actions operate on the MULTI-SELECTED candidates (119 D-12); anchor always included for Puzzle.

---

## Claude's Discretion

- ACT-02 bulk staging mechanism (staging key vs batched query param).
- Export per-cell text cap value, column ordering, CSV/XLSX UI affordance.
- Choose-from-list picker layout (flat vs drill-down).
- Stop button + "restoring…" indicator placement/styling.
- `/lists` "Open in Joins Lab" glyph + tooltip.
- Compare info-button placement; whether prefetch also warms transcription text.
- Exact `joins_lab` schema additions under `schema_version`.

## Deferred Ideas

- Cloud sync of working state across devices → PST-F1.
- Admin / cross-user join moderation + delete (120 adds self-delete only); no confirm/delete UI exists anywhere — OPEN_ISSUES note.
- Show anchor's saved joins.db joins (SEED-007 #3) — declined (shared-db noise).
- True engine-streaming partial results — not needed; reuse existing partial-on-cancel.
- Full i18n / RTL / Hebrew-leak audit → Phase 121.
