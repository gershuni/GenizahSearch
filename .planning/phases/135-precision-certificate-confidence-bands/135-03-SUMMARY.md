---
phase: 135-precision-certificate-confidence-bands
plan: 03
subsystem: discovery
tags: [discovery, cert01, precision-certificate, protocol, pre-registration, masking, statistics]

# Dependency graph
requires:
  - phase: 134-discovery-data-spine
    provides: discovery-sidecar-schema-v1.md (two-table claim model, frozen enums, display-evidence precedence lattice), discovery-band-labels-v1.md (band wordings + Lever-1 coverage bands)
  - phase: 135-01
    provides: shared/discovery_band_labels.py::STRICT_FLOOR = 0.85 (the code-side constant this protocol's decision rule targets)
provides:
  - A tracked, committed, masking-clean written protocol (docs/specs/discovery-cert01-protocol.md) fixing every CERT-01 measurement parameter before any card is drawn
  - The frozen dedup/ranking SQL resolving post-canonical-merge (page_id, canonical_work_id) collisions, reusing the sidecar's own display-evidence precedence lattice
  - The frozen page->physMS cluster mapping + its cluster_map_hash recipe (closes the Codex #13 gap where a witness_units rebuild could silently change the CI without moving frame_content_hash/population_hash)
  - The immutable pre-registration payload discipline (cert01_prereg.json, self-referential report_id, four frozen input hashes + DB content_hash, separate deck manifest, verify_cert01_grading.py recompute contract) for 135-09 to implement against
  - The pre-registered FAIL branch (tested 135-06 reband to screening_rb as a rebuild input + atomic legacy-precision invalidation) and the grading-STARTED phase-closing signal
affects: [135-09 (frame freeze + OC computation + card draw), 135-06 (the tested reband/rebuild path), BAND-05 methods page, CERT-02 display rules]

# Tech tracking
tech-stack:
  added: []
  patterns: ["content-hash-based freeze discipline (never git-commit-ordering)", "self-referential report_id construction (hash payload minus its own field, then insert)", "reuse-the-E1-harness-as-is for a new certification round"]

key-files:
  created: [docs/specs/discovery-cert01-protocol.md]
  modified: []

key-decisions:
  - "The estimand's cross-claim dedup SQL reuses the sidecar's own §6 display-evidence precedence lattice verbatim (applied across colliding claims rather than across evidence rows within one claim) instead of inventing a new ranking rule -- keeps exactly one authoritative precedence order in the codebase"
  - "cluster_map_hash is specified as its own dedicated hash (SHA-256 over sorted (page_id, canonical_work_id, unit_key) triples) distinct from frame_content_hash and population_hash, because neither of those two would change if a future witness_units rebuild altered physMS clustering -- yet that would silently move the physMS-clustered bootstrap's CI and the Strict 0.85 pass/fail decision"
  - "The FAIL-branch band_precision invalidation requires a measurement_status column that does not yet exist on the frozen band_precision table (discovery-sidecar-schema-v1.md SS1.6) -- documented as an implementation note for 135-06 (a required versioned schema amendment, or an equivalent NULL-precision + notes representation with an identical observable contract), rather than silently assumed or added to the schema doc in this plan (out of this plan's declared files_modified scope)"
  - "OC-table grid values (p in {0.80, 0.85, 0.90, 0.95} x realized ICC x INS rate) are frozen as a SHAPE only, not computed numbers -- the real cells require the actual v2 frame's realized physMS component structure, which does not exist until 135-09 builds it"

requirements-completed: []  # CERT-01 frontmatter-shared across 135-03/06/09; mirrors the 134-01/02/03 precedent of deferring mark-complete until the measurement itself (135-09) lands

# Metrics
duration: ~50min
completed: 2026-07-24
---

# Phase 135 Plan 03: CERT-01 Pre-Registered Protocol Summary

**A tracked, masking-clean written protocol (`docs/specs/discovery-cert01-protocol.md`) that pre-registers the entire CERT-01 tier-A precision measurement design — frozen estimand SQL, physMS cluster mapping hashed as `cluster_map_hash`, Strict ≥0.85 decision rule, immutable pre-registration freeze with four input hashes + DB content_hash, and a tested FAIL-branch reband to `screening_rb` — before any card is drawn.**

## Performance

- **Duration:** ~50 min
- **Completed:** 2026-07-24
- **Tasks:** 1 completed
- **Files modified:** 1 (new file)

## Accomplishments

- Authored the complete CERT-01 protocol document as the Codex #2 fix: a TRACKED, committed, masking-clean pre-registration (never the gitignored spike tree), reusing the `same_work_spike/probe` E1/Q2 adjudication harness section-heading template (Objective → frozen estimand → decision rule → strata → freeze discipline → OC table → deck → diagnostic sample → outcomes → closing signal → deviations register).
- Froze the estimand precisely per D-05: the shipped, display-deduplicated `(page_id, canonical_work_id)` population sampled AFTER canonical merges, the w001239 drop, Lever-1 coverage routing, and the D-17 chronological demotion — with the EXACT dedup/ranking SQL for post-merge collisions specified (reusing the sidecar's own §6 display-evidence precedence lattice, not a new rule).
- Froze the page→physical-MS cluster mapping (`unit_key = COALESCE(witness_unit_members.unit_id, 'sys:'||sys_id)`) and specified `cluster_map_hash` as its own dedicated hash, explaining exactly why `frame_content_hash`/`population_hash` alone cannot catch a future clustering change (Codex #13).
- Froze a deterministic cross-corpus stratum tie-break (rank sefaria < ja < msource) for canonical works whose merged raw members span more than one source corpus.
- Fixed the ONE decision rule (Strict, physMS-clustered lower bound ≥ 0.85), citing `shared/discovery_band_labels.py::STRICT_FLOOR` as the code-side constant it targets.
- Specified the immutable pre-registration payload discipline: `cert01_prereg.json`'s self-referential `report_id` construction (hash the canonical payload minus its own `report_id` field, then insert the digest), the four frozen input hashes (canonical-merges, composition-dates, seftja-dates SHA-256s + the deployed v2 DB `content_hash`) plus `crosswalk_sha256` and `cluster_map_hash`, a SEPARATE `cert01_deck_manifest.json` referencing that `report_id`, and `verify_cert01_grading.py`'s (135-09) recompute-and-compare contract against the deployed sidecar — explicitly NOT git-commit ordering (Codex #B1/#B3/#13).
- Specified the mandatory pre-outcome OC table (method = `e1_confirm_sizing.py`'s `anova_icc`/`n_det_required`/`wilson_lower_one_sided`/`size_confirmation`), explaining why the Strict 0.85 floor at ~200–250 cards is materially harder than any E1 round has cleared (Pitfall 8).
- Specified a blinded diagnostic sample spanning demoted AND retained `later_shared_text` candidates, joined to the hidden classifier tag only after verdict lock, reported as classifier validation only (never adjudication evidence, D-08).
- Specified the three outcome branches: PASS ("expert-measured · independent audit pending", never "certified"); FAIL (a TESTED 135-06 reband flipping `routing_status` to `review_only` and rebanding to the real frozen key `screening_rb` — never the non-existent `screening` — applied as a rebuild input so `evidence_id`/`display_evidence_id` regenerate rather than a bare in-place `UPDATE`, plus an atomic invalidation of `screening_rb`'s legacy 0.859 precision); and insufficient-evidence/wide-CI (keep `tier_a` non-default pending the confirmation draw, no permanent relabel).
- Defined the phase-closing "grading STARTED" signal as three mechanically-checkable, hash-verifiable artifacts (pre-registration frozen, deck manifest rendered, ≥1 verdict recorded), per Pitfall 7.

## Task Commits

Each task was committed atomically:

1. **Task 1: Author the TRACKED CERT-01 pre-registration protocol (from the E1 template)** - `5f93dfe8` (docs)

**Plan metadata:** (this SUMMARY + STATE/ROADMAP update, committed separately per the final_commit step)

## Files Created/Modified

- `docs/specs/discovery-cert01-protocol.md` - The full pre-registered CERT-01 protocol: frozen estimand + dedup SQL + cluster mapping + cluster_map_hash + stratum tie-break, decision rule, strata/weights, blindness/gold/exclusion rules, immutable pre-registration freeze discipline (report_id, four input hashes, separate deck manifest, verifier recompute contract), pre-outcome OC table method, deck + confirmation draw, blinded diagnostic sample, outcome branches (PASS/FAIL/insufficient-evidence) with the tested reband + legacy-precision-invalidation FAIL action, phase-closing signal, deviations register, cross-references.

## Decisions Made

- The cross-claim dedup SQL for post-merge `(page_id, canonical_work_id)` collisions reuses the sidecar's own §6 display-evidence precedence lattice verbatim (human_confirmed dominance → band-rank → adjudication_status tie-break → evidence_id lexicographic tie-break), applied across colliding claims instead of inventing a second ranking rule — keeps one authoritative precedence order in the project.
- `cluster_map_hash` is specified as its own hash, distinct from `frame_content_hash`/`population_hash`, because a future `witness_units`/`witness_unit_members` rebuild could silently change the physMS-clustered bootstrap's CI (and therefore the Strict 0.85 pass/fail decision) while leaving those two hashes unchanged — this is the exact gap Codex #13 flagged.
- The FAIL-branch's atomic legacy-precision invalidation is specified against a `measurement_status='not_measured'` field that does not yet exist on the frozen `band_precision` table (`docs/specs/discovery-sidecar-schema-v1.md` §1.6). Rather than silently amend that frozen spec in this plan (out of scope — `files_modified` only declares the cert01 protocol doc), the protocol document records this as an explicit implementation note for 135-06: it must land a versioned schema amendment adding the column, or represent invalidation via the existing NULL `precision`/`ci_low`/`ci_high` columns plus a `notes` field — either way the observable contract (a UI reading `screening_rb`'s precision after a FAIL reband must render "not yet measured," never the stale 0.859) is fixed now, in writing, before 135-06 is planned.
- The OC-table section freezes the METHOD and grid SHAPE (`p ∈ {0.80, 0.85, 0.90, 0.95}` × realized ICC × INS rate) rather than computed numbers, because the real cells depend on the v2 frame's realized physMS component structure, which does not exist until 135-09 builds it — consistent with the phase's own sequencing (135-03 is Track A / census-independent; the actual frame freeze + card draw is 135-09).

## Deviations from Plan

None - plan executed exactly as written. The one judgment call above (the `measurement_status` schema-dependency note) is documentation of a forward dependency for a LATER plan (135-06), not a deviation from this plan's own task — the plan's `<action>` text itself already anticipated this exact wording ("sets its band_precision row to `measurement_status='not_measured'`") without asking this plan to modify the frozen schema file, so flagging the gap in writing (rather than silently assuming the column exists, or silently amending a file outside this plan's declared scope) is the correct, conservative execution of the plan as specified.

## Issues Encountered

- **The plan's own automated masking-verify command (`check_atlas_masking.py --scan-repo --strict`) cannot pass on this machine, by construction, independent of this plan's content:**
  1. `--strict` requires BOTH `--scan-repo` AND `--scan-asset PATH` together (a hard argument-parsing gate in `check_atlas_masking.py::main`) — the plan's verify command supplies only `--scan-repo`, so it exits 2 (usage error) before any scanning occurs.
  2. Even `--scan-repo` alone (without `--strict`) fails CLOSED with exit 1 on this machine because `MASKING_SCAN_PATTERNS_FILE` is unset (an owner-only operational secret file, per `docs/specs/discovery-sidecar-schema-v1.md`'s own provenance-masking note and the 134-02 SUMMARY precedent — "R-source token pre-registration in the gitignored MASKING_SCAN_PATTERNS_FILE deferred as an owner-only operational step"). A zero-pattern scan is deliberately treated as a hard error (`_require_patterns`), never a silent pass.
  - This is a PRE-EXISTING environment/usage-plumbing gap, not something this plan's content can fix (not a package install, not a code bug in scope for this plan — `docs/specs/discovery-cert01-protocol.md` is the only file this plan is permitted to touch). Manually verified masking-cleanliness by construction instead: a manual grep for the restricted real name (not reproduced here) confirms the doc uses ONLY the sanctioned codename (5 occurrences, never the restricted real name — `grep "<restricted-real-name>"` found zero matches), a Python scan confirms zero Hebrew-range characters in the file, and every work reference in the doc uses an opaque `w000xxx` id, a bare count, or a numeric year, per the masking constraint.
  - The literal string-presence check in the plan's verify block (the Python one-liner checking for `Strict`/`0.85`/`review_only`/`comp_bootstrap`/`pre-registration`/`later_shared_text`/`witness_unit`/`screening_rb`/`cluster_map_hash`/`content_hash`) DOES pass — confirmed by running it directly (all ten required substrings present).

## User Setup Required

None - no external service configuration required. (Separately, and NOT part of this plan: the owner must eventually populate `MASKING_SCAN_PATTERNS_FILE` with the real R-source/M-source restricted strings for the masking gate to run non-fail-closed in this environment — this was already flagged as a pending owner action in the 134-02 SUMMARY and remains outstanding.)

## Next Phase Readiness

- The written CERT-01 protocol is complete and committed. 135-09 can now write `cert01_prereg.json` + `cert01_deck_manifest.json` + `verify_cert01_grading.py` directly against this document's frozen recipes, once the `discovery-v2` frame exists (blocked on the v2 re-distill, a separate Track-B leadoff task per `135-CONTEXT.md`).
- 135-06 (the tested reband/rebuild path) has an explicit, pre-registered FAIL-branch specification to implement against, including the flagged `band_precision.measurement_status` schema dependency.
- No blockers introduced by this plan. The phase's real blocker (the v2 re-distill / census-driven data quality remediation) is unchanged and tracked separately in `.planning/STATE.md`.

## Self-Check: PASSED

- `docs/specs/discovery-cert01-protocol.md` — FOUND (created, 733 lines).
- Commit `5f93dfe8` — FOUND (`git log --oneline --all | grep 5f93dfe8` confirms).
- Required-substring verify (`Strict`, `0.85`, `review_only`, `comp_bootstrap`, `pre-registration`, `later_shared_text`, `witness_unit`, `screening_rb`, `cluster_map_hash`, `content_hash`) — ALL PRESENT.
- Masking cleanliness — verified by construction (no restricted real name, zero Hebrew characters, only the sanctioned `M-source` codename + opaque `w000xxx` ids/counts/years); the automated `check_atlas_masking.py --scan-repo --strict` gate itself cannot execute in this environment (pre-existing, documented above), consistent with prior-plan precedent.

---
*Phase: 135-precision-certificate-confidence-bands*
*Completed: 2026-07-24*
