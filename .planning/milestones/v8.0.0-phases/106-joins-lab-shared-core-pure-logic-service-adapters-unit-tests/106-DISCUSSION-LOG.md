# Phase 106: Joins Lab Shared Core - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-03
**Phase:** 106-joins-lab-shared-core-pure-logic-service-adapters-unit-tests
**Areas discussed:** Module shape & domain model, SearchExecutor adapter contract, Builder depth, Scope boundary

---

## Area selection

All four offered gray areas selected: Module shape & domain model · SearchExecutor adapter contract · Builder depth (deferral #3) · Scope boundary (deferrals #1/#6/#7).

---

## Module shape & domain model

| Option | Description | Selected |
|--------|-------------|----------|
| Single module `shared/joins_lab.py` | One file, matches shared/*.py convention | ✓ |
| Package with submodules | `shared/joins_lab/` compose/membership/dedup/merge/models/executor | |
| Two modules | logic + adapter split | |

**User's choice:** Single module.

| Option | Description | Selected |
|--------|-------------|----------|
| Typed dataclasses + boundary normalization | Frozen dataclasses; adapter normalizes engine dicts → Candidate | ✓ |
| Dict passthrough (sketch style) | Keep list-of-dict, provenance as dict keys | |
| Hybrid (typed inputs, dict candidates) | | |

**User's choice:** Typed dataclasses + boundary normalization.

---

## SearchExecutor adapter contract

| Option | Description | Selected |
|--------|-------------|----------|
| Narrow: search-engine surface only | execute_search + get_browse_page + get_meta_for_id + get_library_for_id; VS/measurement via shared services | ✓ |
| Wide: one adapter wraps everything | also VS + measurement + image | |
| Two adapters | SearchExecutor + DataProvider | |

**User's choice:** Narrow.

| Option | Description | Selected |
|--------|-------------|----------|
| Adapter returns raw engine dicts; module normalizes once | single source of truth | ✓ |
| Adapter returns Candidates (apps normalize) | per-app normalizer, drift risk | |

**User's choice:** Adapter returns raw dicts; module owns the normalizer.

---

## Builder depth (deferral #3)

| Option | Description | Selected |
|--------|-------------|----------|
| Validated minimal, additive-extensible | term + per-row line START/END + ↓N-gap + global variants; defaults make later fields additive | (superseded by free-text) |
| Pre-add richer fields now (off by default) | | |
| Build the richer behavior now | | |

**User's choice (free text):** "We should add start/end of page, though of course start can be only the attribute of first line, and end only of the last." → validated minimal **plus** page-level START (first row only) / END (last row only); additive-extensible dataclasses retained.

| Option | Description | Selected |
|--------|-------------|----------|
| Keep page-level folded into per-row line anchors | line-level only | (superseded — user wants page-level) |
| Re-expose page-level as a per-builder toggle | | ✓ (refined to first/last-row constraint) |

**User's choice:** Add page-level start/end, constrained to first/last row.

| Option | Description | Selected |
|--------|-------------|----------|
| Defer raw-query preview; compose one-way rows→string | rows are source of truth | ✓ |
| Build editable raw-query in the core | string↔rows round-trip | |

**User's choice:** Defer raw-query preview.

### Follow-up — page-anchor semantics

| Option | Description | Selected |
|--------|-------------|----------|
| Additional, stronger constraint | page-START layered on top of (independent of) per-row line-START; content_head/content_tail | ✓ |
| Page-START replaces line-START on row 1 | either/or per edge | |

**User's choice:** Additional, stronger constraint.

---

## Scope boundary (deferrals #1 / #6 / #7)

| Option | Description | Selected |
|--------|-------------|----------|
| Known-joins grouping stays in 107; 106 excludes | via existing JoinsManager.get_connected_fragments_by_id; pairwise→group for v8 | ✓ |
| Extract a thin known-joins helper into 106 | | |

**User's choice:** Stays in 107; 106 excludes. Pairwise→group confirmed.

| Option | Description | Selected |
|--------|-------------|----------|
| Out of 106; lean = JSA-01 keep, JSA-03 spike, JSA-02 cut | finalize at discuss-phase 110 | ✓ |
| Out of 106; keep ALL of JSA-01/02/03 | | |
| Out of 106; decide JSA disposition entirely at 110 | | |

**User's choice:** Out of 106; lean = JSA-01 keep / JSA-03 spike / JSA-02 cut.

| Option | Description | Selected |
|--------|-------------|----------|
| Confirm p±1; multi-leaf deferred | other side = adjacent image p±1, (sys_id, page±1) membership | ✓ |
| Revisit the p±1 rule now for multi-leaf | | |

**User's choice:** Confirm p±1; multi-leaf deferred.

## Claude's Discretion
- Dataclass field names + internal helper decomposition; FakeSearchExecutor/fixture design; snippet
  centering params + MARK-token internals.

## Deferred Ideas
- Per-row variation columns; editable raw-query preview; richer N-fragment join model; JSA-02 (cut
  lean) / JSA-03 (spike lean) finalized at 110; multi-leaf "other side"; web Joins Lab UI.
- Research flags R-01 (page-level vs line-break composition realization) and R-02 (leading
  tear-bracket tokens defeating line_start) recorded in CONTEXT.md for the researcher.
