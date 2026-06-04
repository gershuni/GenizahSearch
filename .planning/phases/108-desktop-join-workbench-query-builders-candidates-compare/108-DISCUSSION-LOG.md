# Phase 108: Desktop Join Workbench — Query Builders, Candidates & Compare - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-04
**Phase:** 108-desktop-join-workbench-query-builders-candidates-compare
**Areas discussed:** Builders & cross-side, Candidate surface, Compare surface, Assists & readouts

---

## Builders & cross-side

### Q: Builder layout (anchor side + other side)
| Option | Selected |
|--------|----------|
| Stacked, other side collapsed | ✓ |
| Two columns side-by-side | |
| Tabs: This side / Other side | |

### Q: How a query starts (JWB-06 reframe)
| Option | Selected |
|--------|----------|
| Blank rows + pull-from-anchor | ✓ (chose "Blank") |
| Pre-fill last anchor line, editable | |
| Seed-from-direction helper | |

### Q: Builder search-mode controls
| Option | Selected |
|--------|----------|
| Variants toggle + open-in-main-search | |
| Variants toggle only | |
| Per-side mode selector | |
| **Free-text reframe** | ✓ |

**User's choice:** Adapt the existing Responsa **Tabular Query Builder**: Lines scope default;
orientation flipped (line = horizontal row of word-boxes, lines stacked vertically); per-row
"Lines gap: N" + +Add Line; start-of-text/end-of-text options (like main-search selector); full
modifier row + Search Options + Preview. Flagged: the existing Tabular Search EN UI is RTL for the
*components* (wanted) but also RTL for the *rest of the controls* (bug — clipped on the left).
Sketch: `[ ] or [ ] or [ ]  ||  Lines gap: [0]` per line; `[+Add Line]`; Modifiers / Search Options
/ Preview below.

### Q: Cross-side combine default
| Option | Selected |
|--------|----------|
| Default AND, toggle to OR | ✓ |
| Default OR, toggle to AND | |
| No default — ask each time | |

### Q (follow-up): Builder reuse vs new widget
| Option | Selected |
|--------|----------|
| Refactor into a shared widget | |
| New widget, borrow look + compose | ✓ |
| Embed the dialog as-is | |

### Q (follow-up): Per-row semantics
| Option | Selected |
|--------|----------|
| OR-alternatives + gap-down | ✓ |
| Sequential words across the line | |
| Let me clarify | |

### Q (follow-up): RTL/LTR + fix scope
| Option | Selected |
|--------|----------|
| Fix both (via shared widget) | ✓ |
| New builder only | |
| Keep everything RTL in EN too | |

**Notes:** User picked "New widget" (decoupled) yet "Fix both" — interpreted as: build a separate
builder widget, and ALSO fix the existing Tabular Search dialog's RTL chrome as a standalone fix.
Flagged splittable to /gsd-quick if it grows (R-04).

---

## Candidate surface

### Q: Default view + page size
| Option | Selected |
|--------|----------|
| Grid default, 20/page, table toggle | ✓ (added: cards must show highlighted snippet) |
| Table default, grid toggle | |
| Remember last choice | |

### Q: Triage persistence
| Option | Selected |
|--------|----------|
| Persist per anchor, across re-runs | ✓ |
| Ephemeral per search run | |
| Persist to disk across sessions | |

### Q: Triage ↔ collected list ↔ actions
| Option | Selected |
|--------|----------|
| Y = the collected list; act on any | ✓ |
| Separate explicit 'collect'/star | |
| No list — triage filter only | |

### Q: Dimensions (SC#7 confirm)
| Option | Selected |
|--------|----------|
| Soft warning + optional filter | |
| Allow an opt-in hard size filter | ✓ |
| No dimension surfacing | |

**Notes:** Interpreted as soft evidence + mismatch hint (never auto-cull) PLUS an opt-in explicit
min/max filter, off by default — SC#7's "never an *automatic* hard filter" preserved.

### Q: JWB-12 108↔109 seam
| Option | Selected |
|--------|----------|
| Build selector + provenance now, VS stubbed | ✓ |
| Text-only now; add selector in 109 | |

### Q: Default ordering
| Option | Selected |
|--------|----------|
| By engine score, best first | ✓ |
| Group by triage, then score | |
| You decide | |

---

## Compare surface

### Q: Compare host
| Option | Selected |
|--------|----------|
| Separate two-pane CompareDialog | ✓ |
| In-workbench split (reuse pinned anchor) | |
| Inline preview + pop-out | |

### Q: Actions reachable in compare
| Option | Selected |
|--------|----------|
| All four + triage | ✓ |
| Add-as-Join + triage only | |
| View only | |

### Q: Compare nav + folio
| Option | Selected |
|--------|----------|
| Step candidates; open the matched page | ✓ |
| Step candidates, page-1 default | |
| You decide | |

---

## Assists & readouts

### Q: Tear-side assist surfacing
| Option | Selected |
|--------|----------|
| One-line readout near the builder | |
| Inline hint on the anchor transcription | |
| On-demand 'analyze tear' button | |
| **DEFER (free text)** | ✓ |

**User's choice:** "Defer. Right now we are building manual finder. The tear-side assist will help
more algorithmic approach." → JWB-05 deferred out of 108; SC#6 dropped; mapping update needed.

### Q: Tear-side action (inform vs suggest)
**User's choice:** "See above" — moot (assist deferred).

### Q: Self-match readout placement + default
| Option | Selected |
|--------|----------|
| By results header, default EXCLUDE | ✓ (chose "EXCLUDE") |
| By the builder, default EXCLUDE | |
| Default INCLUDE anchor | |

**Notes:** Readout stays; default EXCLUDE anchor (include-toggle OFF); placement = Claude's discretion.

---

## Claude's Discretion
- Self-match readout placement (lean: candidate-list header).
- Builder collapse styling, grid card details, table column widths, CompareDialog sizing/snippet caps.
- Whether the optional "copy selected anchor text → row" affordance ships in 108.
- How much frozen-sketch code transplants.

## Deferred Ideas
- Tear-side assist (JWB-05) → algorithmic approach / Phase 110 / post-v8.
- "Open in main search" escape hatch from the builder.
- VS source population + combined ordering + VS-dialog soft-retire (Phase 109).
- Editable raw composed-query preview (Preview stays read-only).
- Triage persisted to disk across sessions.
- Per-row per-term variants columns; multi-leaf "other side"; web Join Workbench UI.
