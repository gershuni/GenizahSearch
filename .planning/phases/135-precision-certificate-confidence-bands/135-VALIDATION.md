---
phase: 135
slug: precision-certificate-confidence-bands
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-07-23
---

# Phase 135 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Discovery-only surface (no Qt, no Tantivy) — the phase gate is the
> discovery-scoped pytest run PLUS the strict masking gate (DATA-05).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (project-wide; no dedicated discovery-specific config) |
| **Config file** | `pyproject.toml` → `[tool.pytest.ini_options]` (project root; registers the `slow` marker, no default-exclude addopts). No dedicated discovery config — the existing `tests/test_discovery_*.py` suite + `tests/fixtures/discovery/discovery-v1-fixture.db` are the direct precedent to extend. |
| **Quick run command** | `pytest tests/test_discovery_bands.py tests/test_discovery_ids.py -q` (no Qt, no Tantivy — fast, safe to run per-commit) |
| **Full suite command** | `pytest tests/test_discovery_*.py tests/render_smoke/ -q` (discovery-scoped — this phase's own gate) |
| **Project-wide full suite** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/ -q` (CI-only per CLAUDE.md's Qt/Tantivy caveats; NOT required for this phase's gate — no Qt/Tantivy code changes here) |
| **Estimated runtime** | ~5–15s quick · ~60–90s discovery-scoped full |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_discovery_band_labels.py tests/test_discovery_ids.py -q` (Track A) or the relevant new `tests/test_discovery_v2_bake.py::test_<case>` (Track B) — whichever the task touched.
- **After every plan wave:** Run `pytest tests/test_discovery_*.py tests/render_smoke/ -q` (full discovery-scoped suite).
- **Before `/gsd:verify-work`:** Full discovery-scoped suite green **AND** `python scripts/check_atlas_masking.py --scan-repo --scan-sqlite <v2.db> --scan-asset <v2.db> --strict` exit 0 (mandatory hard release gate per DATA-05, not merely a test).
- **Max feedback latency:** ~15s (quick per-commit run).

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 135-01-01 | 01 | 1 | BAND-02 | T-135-01-04 | band_precision reader fail-closed (returns None, never raises); async off-loop | unit | `pytest tests/test_discovery_band_labels.py -k precision -x -q` | ❌ W0 | ⬜ pending |
| 135-01-02 | 01 | 1 | BAND-01, CERT-02 | T-135-01-01 / T-135-01-02 | word gate holds in labels; tier_a NULL → "not yet measured", never a bare number | unit | `pytest tests/test_discovery_band_labels.py -x -q` | ❌ W0 | ⬜ pending |
| 135-01-03 | 01 | 1 | BAND-01, BAND-02, BAND-03, BAND-04, CERT-02 | T-135-01-01 / T-135-01-03 | drift guard (labels ⇔ frozen enum); data-driven copy; word gate over rendered output | unit | `pytest tests/test_discovery_band_labels.py tests/test_discovery_ids.py -q` | ❌ W0 | ⬜ pending |
| 135-03-01 | 03 | 1 | CERT-01 | T-135-03-01 | pre-registration fixes estimand/strata/seed/gate BEFORE any draw | doc-artifact | `test -f same_work_spike/probe/results/PLAN-cert01-tier_a.md && grep -q "0.85" same_work_spike/probe/results/PLAN-cert01-tier_a.md && grep -q "review_only" same_work_spike/probe/results/PLAN-cert01-tier_a.md && grep -q "comp_bootstrap" same_work_spike/probe/results/PLAN-cert01-tier_a.md && echo OK` | ❌ W0 (artifact) | ⬜ pending |
| 135-04-01 | 04 | 1 | CERT-01 | T-135-04-02 / T-135-04-03 / T-135-04-04 | no resurrected `work_relations`; DELTA=100y hardcoded + cited; masking clean | doc-artifact | `grep -q "later_shared_text" docs/specs/discovery-v2-bake-plan.md && grep -q "DELTA" docs/specs/discovery-v2-bake-plan.md && grep -q "canonical_work_id" docs/specs/discovery-v2-bake-plan.md && ! grep -q "work_relations" docs/specs/discovery-v2-bake-plan.md && python scripts/check_atlas_masking.py --scan-repo --strict && echo OK` | ✅ (scripts) | ⬜ pending |
| 135-04-02 | 04 | 1 | CERT-01 | T-135-04-01 / T-135-04-02 | Codex re-review BLOCKING gate before any code; masking over review + tmp | checkpoint:human-verify + doc-artifact | `test -f .planning/phases/135-precision-certificate-confidence-bands/135-BAKEPLAN-CODEX-REVIEW.md && python scripts/check_atlas_masking.py --scan-repo --strict && echo OK` | ❌ W0 (review doc) | ⬜ pending |
| 135-02-01 | 02 | 2 | BAND-05 | T-135-02-02 / T-135-02-04 | per-band copy via values module (no inline number); masking clean | render-smoke | `pytest tests/render_smoke/test_help_methods_render_smoke.py -x -q` | ❌ W0 | ⬜ pending |
| 135-02-02 | 02 | 2 | BAND-05 | T-135-02-01 / T-135-02-03 | `/help` noindex flips with flag; RTL; rendered prose rejects "certified" | render-smoke | `pytest tests/render_smoke/test_help_methods_render_smoke.py -q` | ❌ W0 | ⬜ pending |
| 135-05-01 | 05 | 2 | BAND-01, CERT-01 | T-135-05-01 | enum/DDL/verifier lockstep; DDL CHECK rejects any 6th routing_reason | unit | `pytest tests/test_discovery_ids.py -q` | ✅ (extended) | ⬜ pending |
| 135-05-02 | 05 | 2 | BAND-01, BAND-02 | T-135-05-04 | 0 live `expert_verified`; dual-key values test still green | unit | `pytest tests/test_discovery_band_labels.py tests/test_discovery_bands.py -q` | ❌ W0 / ✅ | ⬜ pending |
| 135-05-03 | 05 | 2 | CERT-01 | T-135-05-02 / T-135-05-03 | dated amendment only (no silent frozen-block edit); golden test extended | unit + doc | `pytest tests/test_discovery_ids.py -q && grep -q "later_shared_text" docs/specs/discovery-sidecar-schema-v1.md && python scripts/check_atlas_masking.py --scan-repo --strict && echo OK` | ✅ | ⬜ pending |
| 135-06-01 | 06 | 3 | CERT-01 | T-135-06-04 | merges applied FIRST; drop-list excluded; no self-erasure of works | unit | `pytest tests/test_discovery_v2_bake.py -k "merge or drop" -x -q` | ❌ W0 | ⬜ pending |
| 135-06-02 | 06 | 3 | CERT-01 | T-135-06-01 / T-135-06-02 / T-135-06-03 | Lever-1 before D-17; never-orphan-shipped; unknown-date never demoted; per-span | unit | `pytest tests/test_discovery_v2_bake.py -k "coverage or demot or orphan or unknown_date" -x -q` | ❌ W0 | ⬜ pending |
| 135-06-03 | 06 | 3 | CERT-01 | T-135-06-02 / T-135-06-05 | verifier invariants (never-orphan, routing_status tier, v1-enum absence) | unit | `pytest tests/test_discovery_v2_bake.py -q` | ❌ W0 | ⬜ pending |
| 135-07-01 | 07 | 4 | CERT-01 | T-135-07-01 / T-135-07-02 | verifier + strict masking over the REAL built asset, exit 0 required | integration | `python scripts/verify_discovery_sidecar.py discovery_data/discovery-v2-*.db && python scripts/check_atlas_masking.py --scan-sqlite discovery_data/discovery-v2-*.db --scan-asset discovery_data/discovery-v2-*.db --scan-repo --strict && echo OK` | ✅ (scripts) | ⬜ pending |
| 135-07-02 | 07 | 4 | CERT-01 | T-135-07-03 | frame_content_hash recomputed by build helper, never hand-written | doc-artifact | `grep -q "frame_content_hash" docs/specs/discovery-frames-v2.md && grep -q "content_hash" docs/specs/discovery-frames-v2.md && python scripts/check_atlas_masking.py --scan-repo --strict && echo OK` | ❌ W0 (frames-v2 doc) | ⬜ pending |
| 135-08-01 | 08 | 5 | CERT-01 | T-135-08-01 / T-135-08-02 / T-135-08-04 | human-approved, asset-first, deploy-ONCE; on-box content_hash match before atomic rename | checkpoint:human-verify (blocking) → post-deploy artifact check | `test -f .planning/phases/135-precision-certificate-confidence-bands/135-08-DEPLOY-LOG.md && grep -q "content_hash" .planning/phases/135-precision-certificate-confidence-bands/135-08-DEPLOY-LOG.md && echo OK` | ❌ W0 (deploy log) | ⬜ pending |
| 135-09-01 | 09 | 6 | CERT-01 | T-135-09-01 / T-135-09-03 / T-135-09-04 | frame frozen before draw; OC table published; estimand = shipped display-deduped tier_a | research-artifact | `test -f same_work_spike/probe/results/cert01_freeze_manifest.json && python -c "import json;m=json.load(open('same_work_spike/probe/results/cert01_freeze_manifest.json'));assert 'seed' in m and 'frame_content_hash' in m" && grep -q "0.85" same_work_spike/probe/results/cert01_oc_table.md && echo OK` | ❌ W0 | ⬜ pending |
| 135-09-02 | 09 | 6 | CERT-01 | T-135-09-02 | any adapter matches reused E1 fns exactly; graders blind to the demotion tag | unit (presence-gated) | `if [ -f same_work_spike/probe/scripts/cert01_frame_adapter.py ]; then pytest tests/test_cert01_harness_adapter.py -q; else echo "no-adapter-code-path (pure-CLI harness) — documented in SUMMARY"; fi` | ❌ W0 (only if adapter written) | ⬜ pending |
| 135-09-03 | 09 | 6 | CERT-01 | T-135-09-01 | grading STARTED: ≥1 verdict recorded + freeze manifest present (D-02 signal) | research-artifact | `python -c "import json,sys; v=json.load(open('same_work_spike/probe/review/cert01_deck_verdicts.json',encoding='utf-8')); sys.exit(0 if isinstance(v,list) and len(v)>=1 and 'verdict' in v[0] else 1)" && test -f same_work_spike/probe/results/cert01_freeze_manifest.json && echo OK` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

New test files / fixtures the plans introduce (must exist / be scaffolded before the tasks that depend on them run):

- [ ] `tests/test_discovery_band_labels.py` (135-01) — drift guard (labels ⇔ frozen enum), data-driven precision copy, word gate over **both** the static `BAND_LABELS` dict AND the rendered output of `format_precision_copy()` / `review_overlay()` across all bands (Warning 3), tier_a no-bare-number, D-11/D-12 constants.
- [ ] `tests/render_smoke/test_help_methods_render_smoke.py` (135-02) — modeled on `tests/render_smoke/test_atlas_render_smoke.py`; BAND-05 render (all 7 bands, full field set, per-band anchors), flag ON/OFF gating, HE RTL, and a case-insensitive regex rejecting "certified" (EN + HE equivalents) anywhere in the rendered confidence section (Warning 4).
- [ ] `tests/test_discovery_v2_bake.py` (135-06) — new file (extends the `tests/test_discovery_build.py` / `test_discovery_schema.py` precedent); fixture rows exercising a merge pair, the w001239-equivalent drop, a low-coverage row, and a synthetic chronological-demotion cluster (never-orphan-shipped, unknown-date-never-demoted, merge-before-chrono ordering) + the new verifier invariants.
- [ ] `tests/fixtures/discovery/` — a v2-analog fixture DB (or added rows) carrying band/merge/demotion/routing_status cases for 135-06.
- [ ] `tests/test_discovery_ids.py` (135-05) — EXISTS; extend with golden-digest / enum-membership assertions once `routing_reason` gains `later_shared_text` (Pitfall 1) and (if Pitfall 2 resolves "yes") the `routing_status` lattice tier.
- [ ] `tests/test_cert01_harness_adapter.py` (135-09) — ONLY if new adapter code (`cert01_frame_adapter.py`) is written to point the E1/Q2 scripts at the v2 frame; skipped (documented in SUMMARY) if the harness runs via pure CLI args with no new Python.

Existing infrastructure covers everything else (`tests/test_discovery_ids.py`, `tests/test_discovery_bands.py`, `scripts/verify_discovery_sidecar.py`, `scripts/check_atlas_masking.py`).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Production v2 asset deploy (135-08 Task 1) | CERT-01 | Human-approved, asset-first, deploy-ONCE production checkpoint (D-04). The go/no-go approval, the on-box masking re-scan against the uploaded bytes, and the atomic-manifest-swap judgment are a blocking human checkpoint — no autonomous path. *(The post-deploy DEPLOY-LOG content_hash match IS automated in 135-08-01.)* | Approve per `docs/specs/discovery-deploy.md`; confirm the DEPLOY-LOG records the matched 135-07 content_hash and a single (never v1-then-v2) deploy. |
| CERT-01 freeze-manifest-BEFORE-draw ordering (135-09 Tasks 1 & 3) | CERT-01 | A research-protocol timestamp/git-commit-ordering discipline, not application code — mirrors how E1's own freeze manifests were verified by inspection, not a unit test. *(The ≥1-verdict-exists + manifest-exists mechanical signal IS automated in 135-09-03; only the temporal ordering is inspected.)* | Confirm the freeze manifest's git-commit timestamp precedes the deck/verdicts artifact timestamps; the manifest fixes seed + frame_content_hash + cutoffs before any card was drawn. |

All other phase behaviors have automated verification.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (Wave 6 / 135-09: Tasks 1 & 3 are genuinely failing-capable automated checks; Task 2 is a presence-gated pytest run — ≥2 of 3 hold unconditionally)
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 15s
- [x] `nyquist_compliant: true` set in frontmatter (`wave_0_complete` stays false — Wave 0 scaffolds are authored by the executors)

**Approval:** pending
