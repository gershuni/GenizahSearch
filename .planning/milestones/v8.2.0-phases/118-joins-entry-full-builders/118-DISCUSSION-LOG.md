# Phase 118: Joins, Entry & Full Builders - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-18
**Phase:** 118-joins-entry-full-builders
**Areas discussed:** Builder UI fidelity, Global toggles & other-side, Known-joins group, "Find joins" entry points, Builder↔transcription interaction, OR-entry within a line, Search mode selectability, Collapsed summary bar content

---

## Builder UI fidelity

| Option | Description | Selected |
|--------|-------------|----------|
| Web-idiomatic, capability-complete | Keep every desktop capability, web-native controls | ✓ |
| Close visual replica of the Qt builder | Reproduce desktop layout faithfully | |
| You decide | — | |

**User's choice:** Web-idiomatic. Free to diverge from desktop look. Responsa mode = default; explicit Text Position; line-based builder shown by default; lines stacked vertically (one above another, not side-by-side); other options tucked away; builder compacts to one line when searching.

### Sub-question: meaning of "start/end text box"

| Option | Description | Selected |
|--------|-------------|----------|
| Per-line ⊢/⊣ toggles, visible | Surface line-start/end on each row | |
| Two dedicated boxes (start/end of line) | Torn-edge shortcut | |
| Both — explain | — | |

**User's choice (free-text):** "In advanced options in search hides the option 'Text Position'. It's crucial for joins." → Surface the existing `/search` Text Position control (anywhere/start/end/line_start/line_end), currently buried in advanced.

### Sub-question: collapse behavior on search

| Option | Description | Selected |
|--------|-------------|----------|
| Auto-collapse to a summary bar, re-expandable | Collapses on search; Edit to expand | ✓ |
| Manual collapse toggle | User-driven only | |
| You decide | — | |

**User's choice:** Auto-collapse to a summary bar, re-expandable.

---

## Global toggles & other-side

| Option | Description | Selected |
|--------|-------------|----------|
| One shared set, applied to both sides | Single panel applied to anchor + other side | ✓ |
| Per-side toggles (desktop parity) | Each builder its own state | |
| You decide | — | |

**User's choice:** One shared set, applied to both sides.

### Sub-question: other-side default + options placement

| Option | Description | Selected |
|--------|-------------|----------|
| Off + collapsed by default; options in an 'Advanced' disclosure | Checkbox-gated other side, Narrow default | ✓ |
| Other-side visible by default | Shown up front | |
| You decide | — | |

**User's choice:** Off + collapsed by default; options in an "Advanced" disclosure.

---

## Known-joins group

| Option | Description | Selected |
|--------|-------------|----------|
| In/under the sticky anchor pane | Joins stay in view with the anchor | ✓ |
| Top of the scrolling work column | More room, scrolls away | |
| You decide | — | |

**User's choice:** In/under the sticky anchor pane.

### Sub-question: interactions this phase

| Option | Description | Selected |
|--------|-------------|----------|
| Display + re-anchor + browse link | Group + click-to-re-anchor + open in /browse | ✓ |
| Display only | Source attribution only | |
| You decide | — | |

**User's choice:** Display + re-anchor + browse link (bulk puzzle/list → Phase 120).

### Sub-question: multitenant safety (ANC-05)

| Option | Description | Selected |
|--------|-------------|----------|
| Public/confirmed-only in the shared path | Structurally leak-proof | ✓ |
| User/status-aware cache isolation | See own unconfirmed joins | |
| You decide | — | |

**User's choice:** Public/confirmed-only in the shared path.

---

## "Find joins" entry points

| Option | Description | Selected |
|--------|-------------|----------|
| New browser tab | Preserve search/browse context | ✓ |
| Navigate in place | Replace current page | |
| You decide | — | |

**User's choice:** New browser tab.

### Sub-question: label/icon

**User's choice (free-text, custom spec):** Reuse the existing Browse joins button. When joins exist → the dialog gains a "Find more joins" button under "View all fragments" (tooltip "go to joins lab to find more joins"). When no joins → button in a different color, tooltip "Find Joins". Same in Quick View. Search results: show the joins icon.

### Sub-question: visibility/gating

**User's choice:** Every fragment.

### Sub-question: no-joins click behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Go straight to the Joins Lab, new tab | No near-empty dialog | ✓ |
| Open the joins dialog first (near-empty) | Always a dialog | |
| You decide | — | |

**User's choice:** Go straight to the Joins Lab, new tab.

### Sub-question: search-card icon behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Same logic as browse | Dialog when joins exist; straight to Lab when none | ✓ |
| Always straight to the Lab | Skip dialog on cards | |
| You decide | — | |

**User's choice:** Same logic as browse.

---

## Builder ↔ transcription interaction

| Option | Description | Selected |
|--------|-------------|----------|
| Click anchor line → seed a builder row | Copy line text into a row | |
| Builder always typed by hand | No seeding | ✓ |
| You decide | — | |

**User's choice:** Builder is always typed by hand.

---

## OR-entry within a line

| Option | Description | Selected |
|--------|-------------|----------|
| Per-line text field with light Responsa syntax | space = word, a/b = OR | ✓ |
| Chip-based OR slots | Chips per slot | |
| Multi-box [+or] (desktop replica) | Parallel word-boxes | |

**User's choice:** Per-line text field with light Responsa syntax.

---

## Search mode selectability

| Option | Description | Selected |
|--------|-------------|----------|
| Responsa default + toggles, no mode selector | Toggles only | |
| Expose a mode selector (exact / variants / fuzzy) | Mode control alongside toggles | ✓ |
| You decide | — | |

**User's choice:** Expose a mode selector (exact / variants / fuzzy).

**Notes / correction:** User clarified the toggle/mode model: "Variants is not redundant — it combines with Responsa (adds a layer). But Judeo-Arabic is not needed." → Variants lives in the mode selector layered on Responsa; advanced toggles become Flexible-spacing + Bidirectional; **Judeo-Arabic dropped** (documented divergence from BLD-04 + desktop parity). User confirmed the corrected model. User also requested a **per-line "distance to next line" gap control** (gap_to_next → `[|N]`), surfaced inline between rows.

---

## Collapsed summary bar content

| Option | Description | Selected |
|--------|-------------|----------|
| Readable summary: mode · line count · key options | Human-readable | ✓ (Claude's discretion) |
| Composed query string (raw) | Precise but cryptic | |
| You decide | — | ✓ |

**User's choice:** "You decide" → Claude will use a readable summary (e.g. `Variants · 5 lines · Text Position: line ends · flex-spacing on`) + empty-builder search guard.

---

## Claude's Discretion

- Exact collapsed summary-bar wording + empty-builder hint text.
- The per-row "more" affordance widget (popover vs inline) and OR text/chip rendering.
- Placement/styling of the per-line gap control between stacked rows.
- Joins icon glyph + colors (joins-present vs none) on cards and the recolored browse button.
- `safe_storage` builder-state shape (full persistence is Phase 120).

## Deferred Ideas

- Bulk Add-to-Puzzle / Add-to-List from the known-joins group → Phase 120 (ACT-02/03).
- Full builder/triage/filter persistence + re-run-on-restore → Phase 120 (PST-01..03).
- Candidate triage / table / Compare / Visual Similarity → Phase 119.
- Anchor-line-click → seed builder row — considered and declined; possible later polish.
