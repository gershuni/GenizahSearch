---
phase: 136
status: passed
verdict: closed-on-evidence
method: retrospective-attestation
verifier_run: false
attested: 2026-08-20
attested_by: "owner-directed roadmap re-map — NOT a gsd-verifier run"
caveat: "136-13 has no SUMMARY (work is in production); ROADMAP criterion 5 was only partially met — /catalog-browse moved to 136.1."
---

# VERIFICATION — Phase 136: Read Surfaces — Connections Panel & Work→Witnesses

> **THIS IS A RETROSPECTIVE ATTESTATION, NOT A `gsd-verifier` RUN.**
> Written 2026-08-20 during the second roadmap re-map. No goal-backward verifier agent was
> spawned for this phase, no VERIFICATION agent analysed the codebase against the phase goal,
> and nothing here was produced by `/gsd-execute-phase`'s verification gate. What follows is the
> evidence the phase's closure actually rests on — production behaviour, commit references, and
> the plan summaries on disk — recorded so that the planning tooling stops reporting a phase as
> incomplete when the record and the live site both say otherwise.
>
> **Verdict: CLOSED ON EVIDENCE.** Treat it as an owner-level attestation, not a machine check.
> If a genuine verifier pass is ever wanted here, run `/gsd-execute-phase 136` — it resumes at
> the verification gates and does not re-run plans that already have a SUMMARY.

**Phase**: 136 — Read Surfaces — Connections Panel & Work→Witnesses
**Closed**: 2026-08-08
**Plans**: 21 of 22 have a SUMMARY.md — `136-13` does not
**Attested**: 2026-08-20
**Method**: retrospective, from production state + git history + on-disk plan summaries

---

## What the closure rests on

| Claim | Evidence | Grade |
|---|---|---|
| Both launch surfaces are publicly live | `/computed-identifications` and the browse connections panel have been publicly reachable since `DISCOVERY_ENABLED=1` on 2026-08-08 (`04434714`); v9.0.0 released 2026-08-16 | production |
| The findings numbers are real and internally consistent | 9,523 = 4,152 + 3,873 + 1,498 over 6,755 manuscripts, on the single `main_pool = 1` basis, with `audience=public`, `sidecar_version=discovery-v1-real`, `data_as_of=2026-08-03`, content hash matching `manifest.deploy.json` | measured |
| Performance was measured, not assumed | 45 combinations enumerated, 41 measured, all PASS through the shipped `_build_findings_query`; worst ordering p95 **334 ms** against a 1,500 ms budget, worst count p95 **105 ms** against 500 ms | measured |
| The masking gate is real and was proven able to fail | Wave 11 / `136-19`: 15 mutations each watched red **by name**; an unset `MASKING_SCAN_PATTERNS_FILE` yields a RED suite, never a skip; coverage derived from what code CALLS and checked at LINE granularity; four classes swept (2,213,790 / 23,063 / 470 / 27,566 chars); six manifest-named databases clean under `--strict --scan-repo --scan-asset --scan-sqlite`; non-vacuity proved by feeding values read OUT of the deployed artifact back in (11 / 26,480 / 1 hits) | measured |
| The four unticked plans' functionality is in production | `136-13`'s rebuild and redeploy (the asset the site serves), `136-15`'s `shared/discovery_panel_model.py`, `136-21`'s `get_work_witnesses` / `build_work_expansion_count_sql` in `shared/discovery_service.py`, `136-22`'s launch-statistics reader (`a4ce0b31`) | source + production |

## What this attestation does NOT establish

- **`136-13` has no SUMMARY.** Its files list is the rebuild-gates and compatibility-attestation
  documents plus `docs/specs/discovery-frames-v2.1.md` and `docs/specs/discovery-budgets.md`. The
  v2.1 asset is built and deployed, so the work is in production — but the plan was never summarised.
  Phase 140 SC3 owes that.
- **ROADMAP criterion 5 was only PARTIALLY met.** `/catalog-browse` integration and neutral-title
  search moved to Phase 136.1 on 2026-08-02 and the criterion text was never updated. Recorded, not
  counted as met.
- Do **NOT** run `gsd-sdk roadmap update-plan-progress 136` blindly — it ticks the halted `136-09`.

## The characteristic defect, preserved here because it outlives the phase

Seven instances, all failing in the same direction — toward false confidence: (1) a masking test that
SKIPPED when its pattern file was absent; (2) an atlas browser-DOM capture exiting 0 having run none
of its interactions; (3) an executor-dispatch assertion measuring ZERO and passing; (4) a benchmark
whose non-empty assertion counted the ROWS of a `COUNT(*)`; (5) a skip list calling a reachable state
"unreachable", hiding a real user-facing crash; (6) a masking capture whose "derived" coverage was a
NAMING CONVENTION, missing 78 executable lines including every surface painted by a click; (7) a
restricted-pattern leak introduced BY the tool built to detect such leaks (`assert needle in text` is
rewritten by pytest to display both operands).

Three were found by the external reviewer, four by CI or direct measurement. **None was found by
reading the code and thinking it looked right.** An eighth instance landed on 2026-08-19 outside this
phase — a build discarding 14 approved works without saying so (`32e2a8d7`).
