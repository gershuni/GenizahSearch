---
phase: 134
slug: discovery-data-spine
status: final
nyquist_compliant: true
wave_0_complete: true
created: 2026-07-21
updated: 2026-07-21
---

# Phase 134 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `134-RESEARCH.md` → "Validation Architecture". The planner wires the
> Per-Task Verification Map with concrete task IDs; the phase-level infra/sampling/Wave-0
> rows below are authoritative. Updated 2026-07-21 for the Codex pre-flight rework
> (D1–D7 / DC1–DC14), the round-2 rework (D3, DC3/N2, DC5, DC7, DC12, DC13, N1, N3, N4, N5, N6),
> the round-3 polish (F1 lazy providers + import-before-load; F2 projection band-filter + anchor;
> F3 staged pre-swap verify; F4 source_corpus cross-table consistency),
> AND the REWORK (2026-07-21, ground-truth data investigation) F1–F16:
> F1 E1 files are DISJOINT independent populations (own o0/o1 offsets, NOT precedence over track1_matches which has spans_json not alen/offsets);
> F2 shared_text has one-side occ offsets + seed_page (no b-side offsets, no flank_class/ge3/cluster_size/router_bucket/rung);
> F3 the LITERAL corroborated predicate (_bucket=='witness' AND is_new AND NOT impurity AND trials>=2; impurity == runner_up>=0.5*support AND support>0);
> F4 scan_sqlite(db_path, patterns) single signature; F5 emission-only audit analog + NEW approved-reader;
> F6 NEW essential back-edge guard (web/nicegui/fastapi, Import+ImportFrom+guarded); F7 shared_text = evidence_source=propagated + not_evaluated (collision rule);
> F8 DROP "one band per claim key" → evidence-combination invariants; F9 routing matrix (family routers → review_only/co_citation);
> F10 status matrix (registry-gated, no audit_passed, no "certified"); F11 physical-MS dedup;
> F12 FK ownership (UNIQUE(claim_id,evidence_id) + display-pointer belongs-to-same-claim);
> F13 band_precision populated BEFORE hashing; F14 bench flag-bypass readiness predicate + nonzero-result; F15 lazy conn; F16 work_id 1:1 crosswalk + canonical_work_id default.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing, `tests/`); golden-fixture precedent in `tests/atlas_bake/` |
| **Config file** | none discovery-specific — Wave 0 adds fixture + verifier + test files |
| **Quick run command** | `pytest tests/test_discovery_*.py tests/test_masking_sqlite.py tests/test_no_back_edges_discovery.py -x` |
| **Full suite command** | `pytest tests/` (Windows: marker-based GUI split per `feedback_full_suite_testing_windows`; discovery tests are non-GUI) |
| **Estimated runtime** | ~30–60 s (discovery subset); minutes for full suite |

**Masking gate is a first-class validation surface** (not just pytest). D4: `--scan-sqlite`
is a first-class `--strict` surface, so the VALID ship-gate invocation is:
`MASKING_SCAN_PATTERNS_FILE=<gitignored patterns> python scripts/check_atlas_masking.py --scan-sqlite <DB> --scan-asset <DB> --scan-repo --strict` must exit 0.
The FROZEN function signature is `scan_sqlite(db_path, patterns)` (patterns list, mirroring `scan_asset(path, patterns)`; builds the matcher internally — F4).
The shipped-DB scan is the BLOCKING gate (DC13); the gitignored review/approved artifacts get only a
REGISTERED-TOKEN defense-in-depth `--scan-asset` (surfaces, does NOT block).
**N6 — `<DB>` MUST be the EXACT filename resolved from a manifest (asset_basename →
`discovery_data/<asset_basename>.db`; the STAGED manifest for pre-swap verify, the LIVE manifest
afterwards), NEVER a native-arg shell glob like `discovery-v1-*.db` (this repo's primary shell is
PowerShell, which does not expand a glob for a native program — the literal `*` would reach Python as
a nonexistent path).**

**Standalone verifier is a first-class release gate** (DC7):
`python scripts/verify_discovery_sidecar.py <DB> --expected-frame-hash <expected>` must exit 0 — the SAME
path-parameterized verifier CI runs over the fixture is run over the real DB in 134-07. It runs ALL invariants
(column allowlist; the VALID (claim_type × evidence_source × confidence_band) combinations; no duplicate
(claim_id, evidence_id); nonempty evidence per claim; exactly one display_evidence_id BELONGING to its claim [F12];
F4 source_corpus cross-table consistency; per-side drift with nullable shared_text b-side offsets [F2/N1];
integrity/FK; release-contract counts). The frame-hash check takes the EXPECTED hash as input (fixture CI mode
passes the pinned golden; real-DB mode passes the value in the manifest/`discovery-frames.md`) and requires
recomputed == expected == `meta.frame_content_hash` (membership-based recipe, DC3/N2). `<DB>` is the exact
manifest-resolved filename (N6).

---

## Sampling Rate

- **After every task commit:** `pytest tests/test_discovery_*.py -x` + masking `--scan-sqlite` on any freshly built fixture DB.
- **After every plan wave:** full discovery test set + `check_atlas_masking.py --scan-repo` (committed-content leak check).
- **Before `/gsd:verify-work`:** the VALID strict masking gate (`--scan-sqlite <DB> --scan-asset <DB> --scan-repo --strict`, `<DB>` = exact manifest-resolved filename) over the real built `.db` + `scripts/verify_discovery_sidecar.py <DB> --expected-frame-hash <manifest/discovery-frames.md value>` exit 0; loader fail-open matrix green (incl siblings-ignored); overload + slot-recycling + web-composition (incl import-before-load + lazy conn) tests green.
- **Max feedback latency:** < 60 s for the discovery subset.

---

## Per-Task Verification Map

> Task ID / Plan / Wave filled from the FINAL 8-plan / 5-wave breakdown. Requirement → observable-signal → command rows
> are pre-derived from research + the Codex-rework fixes (incl the F1–F16 ground-truth data rework). Status is execution-time (⬜ until the owning plan runs).

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| Task 2 (test) / Task 1 (mode) | 134-02 | 1 | DATA-05 | T-134-leak | No M-source/R-source string in sidecar/repo/surface (schema + every cell, str + BLOB); `scan_sqlite(db_path, patterns)` single signature (F4) | integration (CI gate) | `check_atlas_masking.py --scan-sqlite <DB> --scan-asset <DB> --scan-repo --strict` (exit 0; real DB run in 134-07 T2; `<DB>` = exact manifest-resolved filename, N6) | ❌ (134-02) | ⬜ pending |
| Task 2 | 134-02 | 1 | DATA-05 | T-134-leak | BLOB/bytes cells scanned, not just str (DC8) | unit | `pytest tests/test_masking_sqlite.py -k blob` | ❌ (134-02) | ⬜ pending |
| Task 2 | 134-02 | 1 | DATA-05 | T-134-leak | Valid `--strict` combination accepted (D4); test calls `scan_sqlite(db, [patterns])` (F4) | unit | `pytest tests/test_masking_sqlite.py -k strict` | ❌ (134-02) | ⬜ pending |
| Task 1 | 134-03 | 2 | DATA-03 | T-134-leak | No EXACT-name `text`/`cat`/`provenance`/raw-`work_id`/`title`/`author`/`genre` reference columns (text_layer/text_layer_a/text_layer_b + source_corpus allowed); only offsets + snapshot hash | unit | `pytest tests/test_discovery_schema.py::test_no_reference_columns` | ❌ (134-03) | ⬜ pending |
| Task 1 | 134-03 | 2 | DATA-03 | T-134-tamper | Per-side drift: a-side text_layer + snapshot_hash non-null on EVERY evidence row; shared_text b-side other_page_id + snapshot_hash_b non-null but b_start/b_end NULLABLE (F2); a nulled a-side snapshot_hash FAILS (N1) | unit | `pytest tests/test_discovery_release_contract.py -k drift` | ❌ (134-03) | ⬜ pending |
| Task 1 (verify) / Task 2 (corrupt) | 134-03 | 2 | DATA-03 | T-134-tamper | `discovery_claim.source_corpus` == the joined `works.source_corpus`; a valid-but-mismatched code FAILS (F4) | unit | `pytest tests/test_discovery_release_contract.py -k source_corpus` | ❌ (134-03) | ⬜ pending |
| Task 2 | 134-03 | 2 | DATA-01/08 | T-134-tamper | UNIQUE(claim_id, evidence_id) + display_evidence_id BELONGS to its claim; a cross-claim display pointer FAILS (F12) | unit | `pytest tests/test_discovery_release_contract.py -k ownership` | ❌ (134-03) | ⬜ pending |
| Task 1 (verifier) | 134-03 | 2 | DATA-01/02/03/08/10 | T-134-tamper | Path-parameterized verifier runs ALL invariants over any DB path (incl F4 source_corpus + F12 ownership); frame-hash takes --expected-frame-hash (DC7) | integration | `python scripts/verify_discovery_sidecar.py <fixture-db> --expected-frame-hash <pinned-golden>` exit 0 | ❌ (134-03) | ⬜ pending |
| Task 3 | 134-01 | 1 | DATA-01/02 | — | Deterministic `claim_id`/`unit_id` stable across rebuilds | unit (golden) | `pytest tests/test_discovery_ids.py::test_claim_id_golden` | ❌ (134-01) | ⬜ pending |
| Task 3 | 134-01 | 1 | DATA-03 | T-134-leak | `validate_source_corpus_code` raises on non-codes; opaque work_id never emits M:/J:/REF; NO raw literal committed (DC1) | unit | `pytest tests/test_discovery_ids.py::test_validate_source_corpus_code` | ❌ (134-01) | ⬜ pending |
| Task 3 | 134-01 | 1 | DATA-01/02 | — | `corroborated_predicate` TRUE for two-seed (trials>=2) / FALSE for one-seed (rung, no trials) (F3); `claim_type_for_work_witness` + `resolve_claim_type` TOTAL (F7); NO `claim_type_for_flank` (DC9 superseded) | unit | `pytest tests/test_discovery_ids.py -k "corroborated or routing or display"` | ❌ (134-01) | ⬜ pending |
| Task 1 | 134-04 | 3 | DATA-01 | — | Opaque work_id 1:1 stable across two builds (crosswalk-anchored; absent crosswalk aborts); canonical_work_id defaults to work_id (F16) (DC2) | unit | `pytest tests/test_discovery_build.py -k work_id_stable` | ❌ (134-04) | ⬜ pending |
| Task 1 | 134-04 | 3 | DATA-04 | T-134-leak | Only owner-approved neutral columns reach works; CANDIDATE/APPROVED frozen exact headers + rejection rules + NEW `--from-approved` reader round-trip (F5; D6/DC13/N5) | unit | `pytest tests/test_discovery_build.py -k "approved or header or roundtrip"` | ❌ (134-04) | ⬜ pending |
| Task 2 | 134-04 | 3 | DATA-01/03 | — | track1_direct = 4 DISJOINT source populations (own o0/o1; tier_a offsets from largest spans_json span; shadowed_by IS NULL) (F1); propagated corroborated via the literal predicate + weak one-seed; shared_text = evidence_source=propagated/not_evaluated with ACTUAL attrs + one-side offsets, collision onto witness (F2/F7); source_corpus on the claim (N3); per-side drift populated (N1) | unit | `pytest tests/test_discovery_build.py -k "claims or propagated or shared_text or corroborated"` | ❌ (134-04) | ⬜ pending |
| Task 2 | 134-04 | 3 | DATA-02 | — | Registry-gated status matrix (no audit_passed, no "certified") + routing matrix (family routers → review_only/co_citation) (F9/F10) | unit | `pytest tests/test_discovery_build.py -k "routing or status"` | ❌ (134-04) | ⬜ pending |
| Task 2 | 134-04 | 3 | DATA-01 | T-134-tamper | Claims physical-MS-deduped before insert (deterministic representative + supporting_page_ids) (F11) | unit | `pytest tests/test_discovery_build.py -k dedup` | ❌ (134-04) | ⬜ pending |
| Task 3 | 134-04 | 3 | DATA-05/08 | T-134-leak | Blocking DB scan gates finalization; registered-token artifact scan surfaces but does NOT block; band_precision populated BEFORE hashing (F13); DC13 | unit | `pytest tests/test_discovery_build.py -k "orchestrat or artifact or band_precision"` | ❌ (134-04) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-02 | — | Valid (claim_type × evidence_source × confidence_band) evidence combinations; a claim MAY carry multiple bands (one-band-per-key DROPPED, F8); multi-work-per-MS preserved | unit | `pytest tests/test_discovery_bands.py::test_valid_evidence_combinations` | ❌ (134-03) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-02 | — | Fixture logical `frame_content_hash` == pinned golden AND == meta; membership-based (band mutation / claim drop changes it) (DC3/N2/DC10) | integration | `pytest tests/test_discovery_frame.py::test_frame_hash_golden` | ❌ (134-03) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-08 | T-134-tamper | `PRAGMA foreign_key_check` clean + child-row UNIQUE(claim_id,evidence_id) enforced (DC4/F12) | unit | `pytest tests/test_discovery_release_contract.py` | ❌ (134-03) | ⬜ pending |
| Task 2 | 134-06 | 4 | DATA-06 | T-134-dos | Overload → `DiscoveryUnavailable` within timeout; loop stays responsive (never hangs) | async unit | `pytest tests/test_discovery_service.py::test_overload_returns_unavailable` | ❌ (134-06) | ⬜ pending |
| Task 2 | 134-06 | 4 | DATA-06 | T-134-dos | Timed-out heavy slot NOT recycled until the thread finishes (add_done_callback release, not finally) (DC6) | async unit | `pytest tests/test_discovery_service.py::test_timed_out_slot_not_recycled_until_thread_finishes` | ❌ (134-06) | ⬜ pending |
| Task 1 | 134-06 | 4 | DATA-06 | T-134-layer | NEW essential back-edge guard: `shared/discovery_service.py` + `shared/discovery_errors.py` have no module-level web/nicegui/fastapi import (ast.Import + ast.ImportFrom + guarded top-level) (F6/D5) | unit (AST guard) | `pytest tests/test_no_back_edges_discovery.py` | ❌ (134-06) | ⬜ pending |
| Task 1 | 134-06 | 4 | DATA-06 | T-134-stale | ThreadLocalConnection built LAZILY on first use (not in __init__); recreated on path/version change; browse LRU keyed incl sidecar version (F15) | unit | `pytest tests/test_discovery_service.py -k "lru or lazy"` | ❌ (134-06) | ⬜ pending |
| Task 1 | 134-06 | 4 | DATA-10 | — | DiscoveryService unit×work projection: unit shown once at DISPLAYED (highest member) band; enabled-band filter BEFORE pagination; anchor_sys_id excludes the anchor's own unit; members on expansion; same-unit suppressed (N4/F2) | unit | `pytest tests/test_discovery_service.py -k data10` | ❌ (134-06) | ⬜ pending |
| Task 3 | 134-06 | 4 | DATA-06 | T-134-layer | web/discovery.py composes DiscoveryService with the LIVE discovery_available callable + LAZY path/version providers; import-before-load → load fixture → query + correct version (DC12/F1) | unit | `pytest tests/test_discovery_composition.py` | ❌ (134-06) | ⬜ pending |
| Task 3 | 134-05 | 3 | DATA-07 | — | Flag OFF → `discovery_available()` False; all reads no-op | unit | `pytest tests/test_discovery_flag.py::test_flag_off_hides` | ❌ (134-05) | ⬜ pending |
| Task 2 | 134-05 | 3 | DATA-07/08 | T-134-tamper | Sidecar resolved by EXACT asset_basename (siblings ignored, rollback-safe); named-file-absent / hash-mismatch / corrupt / incompatible / missing-meta-key / missing-table / invalid-vocab → `ready=False`; app stays up (DC5/DC11) | unit | `pytest tests/test_discovery_loader.py` | ❌ (134-05) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-08 | — | `PRAGMA integrity_check == ok`; release-contract row counts match actuals; source-DB + crosswalk hash recorded | integration | `pytest tests/test_discovery_release_contract.py` | ❌ (134-03) | ⬜ pending |
| Task 3 | 134-03 | 2 | DATA-10 | — | Units merge Oxford parts + physical joins but NOT `Scribe join`; `unit_id` deterministic; ≤1 unit/sys_id; projection scenario present in fixture (N4) | unit | `pytest tests/test_discovery_units.py::{test_scribe_not_merged,test_unit_id_deterministic}` | ❌ (134-03) | ⬜ pending |
| Task 2/3 | 134-07 | 4 | DATA-02/04/08 | T-134-leak/tamper | Real DB passes the shared verifier (--expected-frame-hash from the manifest) + the VALID strict BLOCKING gate; band_precision inside the hashed DB (F13); `meta.frame_content_hash` == discovery-frames.md == manifest; exact manifest-resolved filename (DC3/DC7/DC8/DC13/N6) | integration (release gate) | `verify_discovery_sidecar.py <DB> --expected-frame-hash <val>` + `check_atlas_masking.py --scan-sqlite <DB> --scan-asset <DB> --scan-repo --strict` exit 0 | ❌ (134-07) | ⬜ pending |
| Task 1 | 134-08 | 5 | PERF-01 | T-134-dos | Measurable query latencies within caps; discovery adds ≤250 MB RSS (measured, exact manifest-resolved DB, flag-bypassing readiness predicate + NONZERO-result assertion + executed-query counts — F14); later-surface caps PENDING (DC14/N6) | measurement | benchmark script (p95 timings) + RSS probe — recorded, not a hard unit gate | ❌ (134-08) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

> Witness-unit merging (DATA-10) is authored in `134-04` Task 3 (`build_witness_units`) and PROVEN against the
> fixture in `134-03` Task 3 (`test_discovery_units.py`, via `scripts/verify_discovery_sidecar.py`); the DATA-10
> unit×work PROJECTION rule (highest-member-band display + enabled-band filter before pagination + anchor-unit
> suppression + same-unit suppression, F2) is exposed + tested in `134-06`
> (`test_discovery_service.py -k data10`) with the fixture projection scenario built in `134-03`.

---

## Wave 0 Requirements

> Wave 0 is DISTRIBUTED, not a single pre-wave: the test infra is created inside each owning plan's
> own wave, dependency-ordered — ids + masking (Wave 1) → fixture + verifier + invariants (Wave 2) →
> loader/flag (Wave 3) → service (Wave 4) → bench (Wave 5). Each item below is allocated to a plan.

- [x] Extend `scripts/check_atlas_masking.py` with a `scan_sqlite(db_path, patterns)` mode (str + BLOB; first-class `--strict` surface; single frozen signature, F4) + `tests/test_masking_sqlite.py`. → **134-02** (Wave 1)
- [x] `scripts/verify_discovery_sidecar.py` (path-parameterized all-invariant verifier incl the VALID evidence-combination check + no-duplicate-evidence-keys + display-pointer ownership F12 + F4 source_corpus cross-table + per-side drift with nullable shared_text b-side N1/F2; frame-hash takes --expected-frame-hash, DC7) + `tests/test_discovery_schema.py`, `test_discovery_bands.py`, `test_discovery_frame.py`, `test_discovery_units.py`, `test_discovery_release_contract.py` — build-output invariants over a small deterministic fixture DB → **134-03** (Wave 2); `test_discovery_ids.py` (golden id recipe + validate_source_corpus_code + corroborated_predicate + claim_type_for_work_witness/resolve_claim_type totality + display lattice) → **134-01** (Wave 1).
- [x] `tests/test_discovery_service.py` (incl DATA-10 projection w/ band-filter + anchor N4/F2 + lazy-conn/versioned-LRU F15) + `tests/test_no_back_edges_discovery.py` (NEW essential layering guard: web/nicegui/fastapi, Import+ImportFrom+guarded, F6) + `tests/test_discovery_composition.py` (web composition + import-before-load lazy resolution, DC12/F1) → **134-06** (Wave 4); `test_discovery_flag.py`, `test_discovery_loader.py` (exact-basename manifest + siblings-ignored + full fail-closed matrix, DC5/DC11) → **134-05** (Wave 3); `test_discovery_release_contract.py` → **134-03** (Wave 2). (Loader tests model on the existing `atlas_assets` tests.)
- [x] A benchmark/RSS script feeding `discovery-budgets.md` (measurable PERF-01 numbers measured, not asserted; flag-bypassing readiness predicate + NONZERO-result assertion + executed-query counts, F14; exact manifest-resolved DB path, no glob; later-surface caps PENDING) → **134-08** (Wave 5); the budgets doc itself created in **134-02** (Wave 1).
- [x] A tiny committed fixture `discovery.db` (+ manifest.json) builder (deterministic, masking-safe synthetic data, frozen constant timestamps; per-side drift fields incl nullable shared_text b-side; witness+shared_text collision; band_precision populated before hash F13; source_corpus on claims matching the parent work; unit×work projection scenario) so CI never needs the 3.1 GB research DB → **134-03** (Wave 2).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| The corrected per-evidence_source band map + the LITERAL corroborated predicate + the ground-truth field/key contract (C-1..C-9 + F1..F3/F7) confirmation | DATA-02 | Domain fact, not a computable invariant; CERT-01 (Phase 135) freezes against this frame | CONFIRMED via the CONTEXT.md CONTRACT CORRECTION (C-1..C-9) + the ground-truth data investigation (this rework: E1 files disjoint, corroborated = the literal two-seed predicate, shared_text one-side offsets). There is NO live checkpoint (134-01 Task 2 is the id/router module, not a checkpoint). 134-01 Task 1 freezes the C-4 band-source map + the corroborated predicate + the work-witness claim_type routing directly; owner confirmation is recorded in CONTEXT.md C-1..C-9 |
| Neutral-title owner review (approve/hand-pick M-source literary subset) | DATA-04 | Human curation gate — fail-closed; owner is the authority (D-06/D-08). Guarantee = only approved neutral columns ship (not a token scan of the artifact) | Owner edits the generated review artifact (CANDIDATE schema); only approved rows (APPROVED schema) re-distill via the NEW --from-approved reader; unreviewed = excluded |
| PERF-01 latency/RSS budgets on the real prod-scale `.db` | PERF-01 | PERF-01 mandates measurement (not assertion); prod box RSS not reproducible in CI; later-surface caps unmeasurable this phase | Run the benchmark/RSS script over the built `discovery.db` (exact manifest-resolved path; flag-bypassing readiness predicate + nonzero-result assertion, F14); record measurable actuals in `discovery-budgets.md`; leave later-surface caps PENDING |
| Deploy: temp-upload → STAGE candidate manifest → verify the STAGED target → ATOMIC live-manifest swap → code | DATA-08 | Requires live server access + asset-first deploy posture | Follow the documented rollback (repoint the live manifest to the prior basename) + rebuild recipe on the web box; pre-swap verify resolves the db from the STAGED manifest (NEVER the live/old one); every command uses the exact manifest-resolved filename (staged pre-swap, live after; no glob; N6); live smoke uses a flag-bypassing readiness probe (flag stays OFF) |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies (checkpoint/human-only tasks carry `<human-check>`)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (distributed across waves, dependency-ordered)
- [x] No watch-mode flags
- [x] Feedback latency < 60s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** 2026-07-21 (updated for Codex pre-flight rework D1–D7 / DC1–DC14 + round-2 rework D3/DC3/DC5/DC7/DC12/DC13/N1–N6 + round-3 polish F1–F4 + the ground-truth data REWORK F1–F16)
