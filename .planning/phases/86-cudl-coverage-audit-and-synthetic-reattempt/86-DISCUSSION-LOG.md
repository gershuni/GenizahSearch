# Phase 86: CUDL Coverage Audit + Synthetic Re-attempt - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the reasoning that shaped them.

**Date:** 2026-05-10
**Phase:** 86-cudl-coverage-audit-and-synthetic-reattempt
**Format:** Conversational discuss-phase (not menu-driven). User reframed the goal early; investigation drove the design.

---

## How the discussion shaped scope

### Round 1: Initial gray-area menu
Claude proposed 4 gray areas (qualification rule, AUDIT-02 structure, AUDIT-03
methodology, UAT + rollback). User declined the menu and asked to clarify first.

### Round 2: User reframe
User: *"From what I've learned, I can create a sophisticated plan but it won't
nail what I really need. So we'll need to do it in a different way. We have a
goal — there are CUDL items that NLI does not have. We have to find them and
create items for them, so we'll show the images and CUDL metadata, with
synthetic sys_ID. We'll have to verify later that it works and exhaustive
enough."*

This inverted the approach from Phase 85's FIST-walked / CUDL-as-filter to
CUDL-walked / FIST-as-resolver. Claude posed three follow-up questions:
1. CUDL with no FIST mapping — handling?
2. FJMS enrichment — bonus or skip?
3. Parent-shadow filter — keep?

### Round 3: User answers
1. *"How many CUDL with no FIST mapping are there?"* — empirical first
2. *"FJMS enrichment is very important"* — keep it
3. *"I'll have to see the data to decide"* — defer to data

### Round 4: Empirical investigation (round 1)
Claude ran probes against `nli_data/nli_crossref.db.cambridge_manifests`,
`fist_data/FIST.db.dbo_Inventory`, and `libraries.csv` via the Phase 84
bridge.

| Metric | Count |
|---|---|
| CUDL classmarks total | 141,368 |
| Resolve via Phase 84 bridge → libraries.csv | 136,038 (96.2%) |
| Truly unresolved (orphan) | 5,330 |
| ↳ WITH FIST inventory (synthesizable in locked format) | 158 |
| ↳ WITHOUT FIST inventory (would need new format) | 5,167 |

Of the 158 synthesizable, 95 were currently blocked by Phase 85's D-05a
STRICT multi_signature rule (including `T-S NS 329.96` — the originating
user case).

### Round 5: User insight
User: *"I guess that most (if not ALL!) of no-FIST id actually do have FIST id,
it's just different exact shelfmark."*

This was the key insight. Claude probed FIST for plausible matches under
different shelfmark forms.

### Round 6: Empirical investigation (round 2)
Probing samples revealed systematic encoding gaps:

| FIST form (real) | CUDL form (real) | Gap |
|---|---|---|
| `Moss. IV,27.1` | `mosseriiv27.1` | `mosseri{roman}` ↔ `moss{roman}` prefix |
| `Mosseri: Moss. IV,27.1` | `mosseriiv27.1` | FIST data-noise prefix |
| `T-S F1(1).11` | `tsf1.11` | `(N)` series-suffix in FIST |
| `Or.1080 11.14` | `or1080.10.14` (similar) | dot-after-1080 in CUDL only |

A normalizer extension covering Mosseri Roman + FIST prefix-strip alone
recovered **3,057 additional matches** (158 → 3,219, 60% of unresolved).
Adding `(N)` series-strip + Or. multi-segment dot-fix recovered another
507 (3,219 → 3,726, 70% of unresolved).

### Round 7: Residue analysis (1,599)
After the 70% normalizer:
- **T-S F (392) + T-S Ar (303):** "flattened-series" hypothesis. CUDL
  appears to encode FIST `(N)` series digit as a leading suffix digit
  (e.g. CUDL `tsf1.1100` may map to `T-S F1(?).100`). Ambiguity warrants
  human adjudication.
- **T-S NS (179):** mix of `minute fragments` aggregated entries
  (recoverable via phrase-suffix rule) + letter-suffix patterns
  (`tsns135.1aa`).
- **Or. (571):** `or1080.X` single-segment vs FIST `Or.1080 X.Y` —
  different fragment granularity? Needs IIIF manifest content
  inspection.
- **Mosseri (48):** exotic letter-suffix patterns.
- **T-S Misc (98):** multi-segment patterns.

Claude offered two postures:
- **Ship 70% + iterate later** (defer 1,599 to Phase 87)
- **Stretch toward maximum coverage with human-in-the-loop adjudication**

### Round 8: User locks the posture
User: *"I'll want to investigate to maximize the mapping — investigation
can use me as human in the loop."*

This shifted Phase 86 from a baseline-coverage phase to a maximum-feasible-
coverage phase. The planner is expected to produce a `86-RESIDUE-PATTERNS.md`
research artifact surfacing the 5 pattern families above with sample
fixtures (CUDL classmark + nearest-neighbour FIST candidates), and the
user adjudicates each pattern before the bridge rules are locked.

---

## Decisions captured (cross-reference CONTEXT.md)

| ID | Decision | Source |
|---|---|---|
| D-01 | CUDL-walked generation (not FIST-walked) | Round 2 reframe |
| D-01a | Image-bearing-only — every synthetic row has CUDL manifest | Round 2 reframe (Phase 85 negative example) |
| D-02 | Bidirectional FIST↔CUDL normalizer (extends Phase 84 bridge) | Round 5 insight, Round 6 investigation |
| D-02a | 4 confirmed normalizer patterns (locked, 70% recovery) | Round 6 empirical validation |
| D-02b | 5 residue patterns for human adjudication (stretch ~70%→90%+) | Round 7 + Round 8 |
| D-02c | Iteration via `86-RESIDUE-PATTERNS.md` research artifact | Round 8 user posture |
| D-04 | D-05a STRICT relax for unambiguous multi_signature (closes T-S NS 329.96) | Roadmap criterion 5(c) + Round 4 confirmation |
| D-04a | multi_inventory stays excluded | Carries from Phase 85 D-05a |
| D-06 | Parent-shadow filter on, even if rare on CUDL-walk | Round 3 user "see the data" → cheap insurance |
| D-07 | FJMS enrichment automatic via Phase 85 UNION-ALL | Round 3 user "very important" + Phase 85 carry |
| D-07a | Web deploy + desktop installer rebuild strategy planner-decided | CLAUDE.md "both apps" + feedback memory |
| D-08 | AUDIT-01 = `scan_cudl_orphans --out-suffix _post_phase86` | Roadmap criterion 1 + Phase 85 verification gap |
| D-09 | AUDIT-02 = `reports/cudl_coverage.md` per-collection + residue analysis | Roadmap criterion 2 |
| D-10 | AUDIT-03 = scan + permanent regression test (golden fixture) | Roadmap criterion 3 |
| D-11 | Phase 85 infrastructure activates as-is, no retroactive changes | Phase 85 verification (5/5 SYNTH-* satisfied at infra level) |
| D-12 | New HUMAN-UAT replaces Phase 85's superseded UAT | Phase 85 supersession + new data |
| D-12a | Rollback = empty marker block + restore .bak — same lever as Phase 85 | Phase 85 revert experience |
| D-12b | No env-var feature flag — data IS the lever | Avoid dormant-flag anti-pattern |

---

## Open items punted to planning

- Choice of normalizer module location (extension vs sibling).
- AUDIT-03 test scope (20 golden rows vs all 461).
- `86-RESIDUE-PATTERNS.md` format.
- Whether to keep Phase 85's `synthetic_ambiguity_residue.csv` populated or rebuild.
- Desktop installer release strategy.

---

## Investigation artifacts (not committed to phase dir, ephemeral)

- `reports/cudl_unresolved_no_fist_sample.csv` (5,167 rows) — generated
  during round 4 probe; superseded once Phase 86 walks the data with
  the new bridge. Will be deleted as part of plan execution; Phase 86's
  own artifacts (`cudl_orphans_*_post_phase86.csv`) replace it.

---

## Deferred Ideas

See CONTEXT.md `<deferred>` section. Highlights:
- CUDL-only no-FIST sys_id allocation (the 5,167 group) → future phase if
  exhaustive coverage wanted.
- Reverse audit (NLI Alma in libraries.csv but absent from CUDL/FJMS).
- Tantivy stub-rows for full-text/Responsa search on synthetic IDs.

---

*Compiled: 2026-05-10*
