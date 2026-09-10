---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 01
subsystem: docs
tags: [discovery, contracts, requirements, schema-doc, deploy-runbook, masking, precision-authorization]

# Dependency graph
requires: []
provides:
  - "Dated 2026-08-02 amendment sub-bullets on REQUIREMENTS.md's NOVEL-01 (candidacy wording, tri-state vocabulary reconciliation), PANEL-01/02 (relation filter, not-collapsed panes, plain-text titles), plus Phase-136.1 forward notes on PANEL-03 (licence-gated reference text) and WORK-01 (tier filter deleted)"
  - "discovery-band-labels-v1.md dated amendment: §2 tooltip-only note, §2 review-overlay-not-rendered note, §3 qualitative-only-everywhere note, §4 human_confirmed/routing-status pre-filter clarification (19/121 all-evidence vs 14/116 display-evidence)"
  - "discovery-budgets.md dated amendment: the Computed-Identifications findings-page cap table with a separate visible-count p95 cap, two PENDING build-time slots, and two new DISCOVERY_* env vars"
  - "discovery-sidecar-schema-v1.md dated amendment: the full new-field contract for the trimmed rebuild (coverage_ppm/coverage_status, band_rank, novelty_status/novelty_source_label, assertion_visibility/identity_visibility, discovery_identification, manuscript_display, display_work_id selection rule, works.genre population rule, meta.audience, the D-10a index set), the narrow §1.6 tier_a authorization amendment with its six-site lockstep list, the discovery_routing_audit demoted_work_id fix, the offset coordinate-space standing rule, and the explicit v2.1 deferral of w_start/w_end + versemap resolution"
  - "discovery-deploy.md correction: the §4 rebuild command now names the live v2 pinned inputs (canonical-merges/composition-dates/seftja-dates + SHA-256), and §2.1/§2.4/§4 gained the externally-pinned-frame-hash requirement"
affects: [136-02, 136-03, 136-04, 136-05, 136-06, 136-07, 136-08, 136-09, 136-11, 136-12, 136-13, 136-19, "136.1"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Dated-amendment discipline on frozen contract docs — every correction is a new `## Amendment YYYY-MM-DD` section, never a silent edit to frozen prose"
    - "Contracts define fields BEFORE any code writes them — schema-first discipline extended to the trimmed rebuild's new columns/tables"

key-files:
  created: []
  modified:
    - .planning/REQUIREMENTS.md
    - docs/specs/discovery-band-labels-v1.md
    - docs/specs/discovery-budgets.md
    - docs/specs/discovery-sidecar-schema-v1.md
    - docs/specs/discovery-deploy.md

key-decisions:
  - "Reconciled the two novelty vocabularies on the record: ROADMAP SC-6's not_in_finding_aids/already_recorded/not_checked is the STORED enum (ROADMAP outranks CONTEXT under this phase's own precedence order); 136-CONTEXT.md D-23a's not_found/known/indeterminate are recorded as descriptive aliases of the same three states"
  - "display_work_id selection rule for the 15-duplicated-canonical_work_id-group fan-out (65,587 rows from a 64,509-row join): canonical anchor first, then lowest source_corpus (sefaria < ja < msource, public-before-private), then lexicographically smallest work_id — an ordered total rule, never 'whichever row the join returns'"
  - "The §1.6 tier_a amendment stores ONLY the authorization (measurement_status='measured_pass', ci_low=0.9084) that is_default_eligible() reads — precision stays NULL, consistent with the no-numbers posture; six lockstep sites enumerated requiring both-branch fixtures"
  - "The deploy runbook's expected-frame-hash source is corrected to an externally pinned value (docs/specs/discovery-frames-v2.md §1, pending plan 136-05's rebuild-preservation baseline) rather than the candidate build's own manifest, which cannot detect a wrong rebuild"
  - "Skipped `requirements mark-complete` for PANEL-01/PANEL-02/VIS-01/NOVEL-01/NOVEL-02 — this plan lands only the contract amendments these later plans (waves 2-9) implement against; marking the shared frontmatter IDs Complete after only this plan would be premature (same precedent as Phase 134's DATA-01/02/03/10 decisions)"

patterns-established:
  - "Every field the trimmed rebuild adds is defined in the frozen schema contract before scripts/build_discovery_sidecar.py writes it — nothing outside the Amendment 2026-08-02 list is authorized to appear in the asset"

requirements-completed: []

# Metrics
duration: 18min
completed: 2026-08-02
---

# Phase 136 Plan 01: Contract & Requirement Amendments Summary

**Five discovery contract documents amended in place (dated `## Amendment 2026-08-02` sections, zero silent edits) to authorize the trimmed rebuild's new fields, the tooltip-only band display, the findings-page PERF-01 budget, and a corrected reproducible rebuild command — landing every contract later Phase-136 plans will cite before any of them write code.**

## Performance

- **Duration:** 18 min
- **Started:** 2026-08-02T08:12:44Z
- **Completed:** 2026-08-02T08:30:28Z
- **Tasks:** 3 completed
- **Files modified:** 5

## Accomplishments

- `.planning/REQUIREMENTS.md`: recorded the shipped candidacy wording set for NOVEL-01 ("Candidates for new finds" / "מועמדים לממצאים חדשים", the declined "new discovery" alternative with its reasoning), the panel-level relation filter + not-collapsed manuscript group for PANEL-01/02, and forward notes on PANEL-03 (licence-gated reference text via the acquisition manifest's `reuse_ok`) and WORK-01 (tier filter deleted) for the work moved to Phase 136.1. Reconciled the ROADMAP-vs-CONTEXT novelty-vocabulary naming clash on the record.
- `docs/specs/discovery-band-labels-v1.md`: four numbered notes — band labels become tooltip-only (frozen `BAND_LABELS` strings and `shared/discovery_band_labels.py` predicates untouched — verified via empty `git diff`), the human-review overlay is computed but never rendered on any Phase-136 surface, precision presentation is qualitative-only everywhere including tooltips, and a contract clarification that `human_confirmed` rows must not be pre-filtered by `routing_status` (citing both the 19/121 all-evidence and 14/116 display-evidence denominators).
- `docs/specs/discovery-budgets.md`: a new "Computed Identifications" findings-page cap table (rows/page, response size, p95, timeout) plus a SEPARATE visible-count p95 cap, justified by the measured 3.41-3.55s ordering and 16s count hazards; two build-time PENDING slots; two new env vars.
- `docs/specs/discovery-sidecar-schema-v1.md`: the complete new-field contract for the trimmed rebuild — `coverage_ppm`/`coverage_status`, `band_rank`, the tri-state `novelty_status`/`novelty_source_label`, `assertion_visibility`/`identity_visibility`, the new `discovery_identification` + `manuscript_display` tables, the `display_work_id` deterministic-representative selection rule (resolving the measured 15-duplicate-group/65,587-row fan-out), the `works.genre` population rule (column already exists — no `ADD COLUMN` migration), `meta.audience`, the D-10a index set, the narrow §1.6 `tier_a` authorization amendment with its six-site lockstep list, the `discovery_routing_audit.demoted_work_id` fix, the offset coordinate-space standing rule, and the explicit deferral of `w_start`/`w_end` + versemap resolution to discovery-v2.1.
- `docs/specs/discovery-deploy.md`: corrected the §4 rebuild command (previously v1-shaped, silently omitting the three v2-specific hash-pinned inputs) to name the live v2 pinned inputs explicitly with their real recorded SHA-256 values from `discovery-frames-v2.md`; added the externally-pinned-frame-hash requirement at §2.1 and §2.4 (the candidate's own manifest cannot vouch for itself), pointing at plan 136-05's forthcoming rebuild-preservation baseline.

## Task Commits

Each task was committed atomically:

1. **Task 1: Amend REQUIREMENTS.md** — `27e812f6` (docs)
2. **Task 2: Amend discovery-band-labels-v1.md + discovery-budgets.md** — `3b576a14` (docs)
3. **Task 3: Define every rebuild field in the schema contract + correct the rebuild command** — `838d1a9c` (docs)

_No plan-metadata-only commit was needed beyond the three task commits above; the final metadata commit (SUMMARY + STATE + ROADMAP + REQUIREMENTS) follows this summary._

## Files Created/Modified

- `.planning/REQUIREMENTS.md` — NOVEL-01/PANEL-01/PANEL-02/PANEL-03/WORK-01 dated sub-bullets
- `docs/specs/discovery-band-labels-v1.md` — `## Amendment 2026-08-02 (Phase 136)`, four notes
- `docs/specs/discovery-budgets.md` — `## 5. Amendment 2026-08-02 (Phase 136)`, findings-page caps
- `docs/specs/discovery-sidecar-schema-v1.md` — `## Amendment 2026-08-02 (Phase 136)`, the (A)-(H) new-field contract
- `docs/specs/discovery-deploy.md` — corrected §4 rebuild command + externally-pinned-frame-hash notes at §2.1/§2.4 + a summary `## Amendment 2026-08-02` section

## Decisions Made

See `key-decisions` in the frontmatter above. The most consequential: the `display_work_id` ordered-total selection rule (canonical anchor → public-before-private source_corpus → lexicographic work_id) is now the frozen contract every later identity join must read, and the tier_a §1.6 amendment stores only a pass/fail authorization, never a number — both decisions were spelled out in the plan and applied verbatim rather than re-litigated.

## Deviations from Plan

None - plan executed exactly as written. All three tasks' automated `<verify>` commands pass, and every acceptance-criteria bullet checked (literal-string presence, `git diff --stat` emptiness on the two build/verify scripts and on `shared/discovery_band_labels.py`, the masking scan on each edited file, the repo-wide masking scan, and the `-k discovery` pytest regression) was independently re-verified after writing.

## Issues Encountered

None. `.masking_patterns` (the gitignored `MASKING_SCAN_PATTERNS_FILE`) was already present locally, so the masking gate ran fail-open (not fail-closed) and every edited file plus the whole repo scanned clean (exit 0) — the plan's fallback "record as NOT MET" path was not needed.

## User Setup Required

None - no external service configuration required. This plan is documentation-only.

## Next Phase Readiness

- Plan 136-02 (methods-page qualitative rewrite) and plan 136-03 (the wave-1 owner-decision checkpoint) can now cite the amended contracts directly.
- Plans 136-05/136-06 (rebuild-preservation gate, tier_a lockstep) and 136-11/136-12 (build wiring) have the frozen field/table definitions to implement against — no column may be written that isn't named in this amendment.
- Plan 136-05 still owes the actual rebuild-preservation baseline artifact; until it lands, the deploy runbook's external frame-hash pin is `docs/specs/discovery-frames-v2.md` §1 (`53725098…`).
- No blockers. `.masking_patterns` is present locally, so later masking-gated plans in this session should not hit the fail-closed path either, but should not assume it is present in every environment.

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-02*

## Self-Check: PASSED

All 5 modified files confirmed present on disk; all 3 task commit hashes (`27e812f6`, `3b576a14`, `838d1a9c`) confirmed in `git log`.
