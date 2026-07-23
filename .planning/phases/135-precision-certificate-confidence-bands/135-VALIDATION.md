---
phase: 135
slug: precision-certificate-confidence-bands
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-07-23
revised: 2026-07-24
---

# Phase 135 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Discovery-only surface (no Qt, no Tantivy) — the phase gate is the
> discovery-scoped pytest run PLUS the strict masking gate (DATA-05).
> Revised 2026-07-24 to fold the Codex pre-flight rework (tracked CERT-01
> artifacts, mechanical grading validator, cross-platform verify commands,
> atomic-manifest-swap deploy, exact-manifest-name resolution).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (project-wide; no dedicated discovery-specific config) |
| **Config file** | `pyproject.toml` → `[tool.pytest.ini_options]` (project root). No dedicated discovery config — the existing `tests/test_discovery_*.py` suite + `tests/fixtures/discovery/discovery-v1-fixture.db` are the direct precedent to extend. |
| **Quick run command** | `pytest tests/test_discovery_band_labels.py tests/test_discovery_ids.py -q` (no Qt, no Tantivy — fast, safe per-commit) |
| **Full suite command** | `pytest tests/test_discovery_*.py tests/test_cert01_*.py tests/render_smoke/ -q` (discovery-scoped — this phase's own gate) |
| **Project-wide full suite** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/ -q` (CI-only per CLAUDE.md's Qt/Tantivy caveats; NOT required for this phase's gate) |
| **Shell** | PowerShell is the primary shell — every `<automated>` command in the plans is a cross-platform Python one-liner or a pytest/script invocation (NO POSIX `test`/`[ -f ]`/`if`/`!`/`&&`-chained-grep, Codex #17). |
| **Estimated runtime** | ~5–15s quick · ~60–90s discovery-scoped full |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_discovery_band_labels.py tests/test_discovery_ids.py -q` (Track A) or the relevant new `tests/test_discovery_v2_bake.py::test_<case>` / `tests/test_cert01_*` (Track B) — whichever the task touched.
- **After every plan wave:** Run `pytest tests/test_discovery_*.py tests/test_cert01_*.py tests/render_smoke/ -q` (full discovery-scoped suite).
- **Before `/gsd:verify-work`:** Full discovery-scoped suite green **AND** the strict masking gate exit 0 (resolved against the EXACT manifest-named v2 db, never a glob — Codex #17): `python -c "import json,subprocess,sys; m=json.load(open('discovery_data/manifest.json',encoding='utf-8')); db='discovery_data/'+m['asset_basename']+'.db'; sys.exit(subprocess.call([sys.executable,'scripts/check_atlas_masking.py','--scan-sqlite',db,'--scan-asset',db,'--scan-repo','--strict']))"` (mandatory hard release gate per DATA-05).
- **Max feedback latency:** ~15s (quick per-commit run).

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 135-01-01 | 01 | 1 | BAND-02 | T-135-01-04 | band_precision reader fail-closed (SELECT *, tolerates the 135-05 registry columns, returns None); async off-loop | unit | `pytest tests/test_discovery_band_labels.py -k precision -x -q` | ❌ W0 | ⬜ pending |
| 135-01-02 | 01 | 1 | BAND-01, BAND-03, BAND-04, CERT-02 | T-135-01-01 / T-135-01-02 / T-135-01-05 | values module + SC#1 serialize_banded_claim (band inseparable) + D-18 is_default_eligible; CI omitted when absent + fail-closed on partial; word gate | unit | `pytest tests/test_discovery_band_labels.py -x -q` | ❌ W0 | ⬜ pending |
| 135-01-03 | 01 | 1 | BAND-01, BAND-02, BAND-03, BAND-04, CERT-02 | T-135-01-01 / T-135-01-03 / T-135-01-05 | drift guard (labels ⇔ frozen enum); data-driven copy; rendered-output word gate; SC#1 inseparability; D-18 predicate table | unit | `pytest tests/test_discovery_band_labels.py tests/test_discovery_ids.py -q` | ❌ W0 | ⬜ pending |
| 135-03-01 | 03 | 1 | CERT-01 | T-135-03-01 | TRACKED pre-registration fixes estimand SQL/physMS-map/stratum-tie-break/strata/seed/gate/manifest-hash-freeze/diagnostic-sample BEFORE any draw; masking clean | doc-artifact | `python -c "import pathlib,sys; t=pathlib.Path('docs/specs/discovery-cert01-protocol.md').read_text(encoding='utf-8'); need=['Strict','0.85','review_only','comp_bootstrap','manifest hash','later_shared_text','witness_unit']; sys.exit(0 if all(n in t for n in need) else 1)" && python scripts/check_atlas_masking.py --scan-repo --strict && echo OK` | ❌ W0 (tracked doc) | ⬜ pending |
| 135-04-01 | 04 | 1 | CERT-01 | T-135-04-02 / T-135-04-03 / T-135-04-04 | NO `CREATE TABLE work_relations` DDL + positive "NO work_relations table"; DELTA=100y; composition-dates CLI + discovery_routing_audit; masking clean (semantic assertion, Codex #1) | doc-artifact | `python -c "import re,pathlib,sys; t=pathlib.Path('docs/specs/discovery-v2-bake-plan.md').read_text(encoding='utf-8'); need=all(s in t for s in ['later_shared_text','DELTA','canonical_work_id','composition-dates','discovery_routing_audit']); no_ddl=re.search(r'create\s+table\s+work_relations', t, re.I) is None; neg=re.search(r'no\s+work_relations\s+table', t, re.I) is not None; sys.exit(0 if (need and no_ddl and neg) else 1)" && python scripts/check_atlas_masking.py --scan-repo --strict && echo OK` | ✅ (scripts) | ⬜ pending |
| 135-04-02 | 04 | 1 | CERT-01 | T-135-04-01 / T-135-04-05 | Codex re-review BLOCKING gate: exact final `VERDICT: APPROVE` bound to the bake-plan SHA-256 (Codex #1); masking over review + tmp | checkpoint:human-verify + doc-artifact | `python -c "import hashlib,re,pathlib,sys; plan=pathlib.Path('docs/specs/discovery-v2-bake-plan.md').read_bytes(); h=hashlib.sha256(plan).hexdigest(); rev=pathlib.Path('.planning/phases/135-precision-certificate-confidence-bands/135-BAKEPLAN-CODEX-REVIEW.md').read_text(encoding='utf-8'); ok=(h in rev) and bool(re.search(r'(?m)^\s*VERDICT:\s*APPROVE\s*$', rev)); sys.exit(0 if ok else 1)" && python scripts/check_atlas_masking.py --scan-repo --strict && echo OK` | ❌ W0 (review doc) | ⬜ pending |
| 135-02-01 | 02 | 2 | BAND-05 | T-135-02-02 | per-band copy via values module; population from the FRAME (not denominator); 0.926 collection-scope only; body card independently gated; masking clean | render-smoke | `pytest tests/render_smoke/test_help_methods_render_smoke.py -x -q` | ❌ W0 | ⬜ pending |
| 135-02-02 | 02 | 2 | BAND-05 | T-135-02-01 / T-135-02-03 | discovery_methods_noindex three-state (pre-release noindex / REL-01 indexed / flag-off indexed); RTL; rendered prose rejects "certified"; patched at web.pages.help + web.main | render-smoke | `pytest tests/render_smoke/test_help_methods_render_smoke.py -q` | ❌ W0 | ⬜ pending |
| 135-05-01 | 05 | 2 | BAND-01, CERT-01 | T-135-05-01 | enum/DDL/verifier lockstep; band_precision registry columns + discovery_routing_audit DDL; routing_reason CHECK rejects a 6th value; audit decision CHECK closed | unit | `pytest tests/test_discovery_ids.py -q` | ✅ (extended) | ⬜ pending |
| 135-05-02 | 05 | 2 | BAND-01, BAND-02 | T-135-05-04 | v1-read-compat: BOTH expert_verified AND high_confidence_algorithmic recognized (v1 key NOT dropped, Codex #8); v1-fixture + values tests green | unit | `pytest tests/test_discovery_band_labels.py tests/test_discovery_bands.py -q` | ❌ W0 / ✅ | ⬜ pending |
| 135-05-03 | 05 | 2 | CERT-01 | T-135-05-02 / T-135-05-03 | dated amendments (schema + routing_audit + band-labels §4 compliant D-18 wording, never "certified" — Codex #19; discovery-frames.md rename note — Codex #8); golden extended | unit + doc | `pytest tests/test_discovery_ids.py -q && python -c "import pathlib,sys; s=pathlib.Path('docs/specs/discovery-sidecar-schema-v1.md').read_text(encoding='utf-8'); b=pathlib.Path('docs/specs/discovery-band-labels-v1.md').read_text(encoding='utf-8'); ok=('later_shared_text' in s and 'discovery_routing_audit' in s and 'CERT-01 gate passes' in b and 'not-default-until-certified' not in b); sys.exit(0 if ok else 1)" && python scripts/check_atlas_masking.py --scan-repo --strict && echo OK` | ✅ | ⬜ pending |
| 135-06-01 | 06 | 3 | CERT-01 | T-135-06-04 | merge map threaded into insert + claim-gen + router (grouped by canonical_work_id); drop before claim-gen; merged-twin no self-demotion (Codex #4) | unit | `pytest tests/test_discovery_v2_bake.py -k "merge or drop or twin" -x -q` | ❌ W0 | ⬜ pending |
| 135-06-02 | 06 | 3 | CERT-01 | T-135-06-01 / T-135-06-02 / T-135-06-03 / T-135-06-06 | composition-date input + coverage gate (Codex #5); Lever-1 before D-17; shipped-universe/no-promotion/Lever-1-provenance (Codex #6); routing_audit; unknown-date fail-safe; FAIL reband (Codex #7) | unit | `pytest tests/test_discovery_v2_bake.py -k "coverage or demot or orphan or unknown_date or reband or lever1 or tie" -x -q` | ❌ W0 | ⬜ pending |
| 135-06-03 | 06 | 3 | CERT-01 | T-135-06-02 / T-135-06-05 | verifier invariants (never-orphan, v1-enum absence, unknown-date, routing-audit replayability — Codex #5) | unit | `pytest tests/test_discovery_v2_bake.py -q` | ❌ W0 | ⬜ pending |
| 135-07-01 | 07 | 4 | CERT-01 | T-135-07-01 / T-135-07-02 / T-135-07-04 | verifier + strict masking over the REAL asset resolved by EXACT manifest name (no glob, Codex #17); composition-date coverage gate passed | integration | `python -c "import json,subprocess,sys; m=json.load(open('discovery_data/manifest.json',encoding='utf-8')); db='discovery_data/'+m['asset_basename']+'.db'; f=m['frame_content_hash']; sys.exit(subprocess.call([sys.executable,'scripts/verify_discovery_sidecar.py',db,'--expected-frame-hash',f]) or subprocess.call([sys.executable,'scripts/check_atlas_masking.py','--scan-sqlite',db,'--scan-asset',db,'--scan-repo','--strict']))" && echo OK` | ✅ (scripts) | ⬜ pending |
| 135-07-02 | 07 | 4 | CERT-01 | T-135-07-03 | frame_content_hash recomputed by build helper; composition-dates + crosswalk SHA-256 recorded (Codex #5 provenance) | doc-artifact | `python -c "import pathlib,sys; t=pathlib.Path('docs/specs/discovery-frames-v2.md').read_text(encoding='utf-8'); sys.exit(0 if all(s in t for s in ['frame_content_hash','content_hash','composition','crosswalk','later_shared_text']) else 1)" && python scripts/check_atlas_masking.py --scan-repo --strict && echo OK` | ❌ W0 (frames-v2 doc) | ⬜ pending |
| 135-08-01 | 08 | 5 | CERT-01 | T-135-08-01 / T-135-08-02 / T-135-08-04 / T-135-08-05 | human-approved atomic MANIFEST swap (candidate + prev manifests; on-box staged masking; Codex #12); deploy log mechanically matches the 135-07 frame hashes | checkpoint:human-verify (blocking) → post-deploy artifact check | `python -c "import re,pathlib,sys; frame=pathlib.Path('docs/specs/discovery-frames-v2.md').read_text(encoding='utf-8'); log=pathlib.Path('.planning/phases/135-precision-certificate-confidence-bands/135-08-DEPLOY-LOG.md').read_text(encoding='utf-8'); hashes=set(re.findall(r'[0-9a-f]{64}', frame)); ok=('manifest.json.candidate' in log and 'manifest.prev.json' in log and 'atomic' in log.lower() and any(h in log for h in hashes)); sys.exit(0 if ok else 1)" && echo OK` | ❌ W0 (deploy log) | ⬜ pending |
| 135-09-01 | 09 | 6 | CERT-01 | T-135-09-01 / T-135-09-03 | TRACKED freeze manifest (report_id = manifest hash; population_hash + stratum counts + gold/confirmation allocations) + OC table before draw; estimand = frozen-SQL shipped tier_a | research-artifact | `python -c "import json,pathlib,sys; m=json.load(open('.planning/phases/135-precision-certificate-confidence-bands/cert01_freeze_manifest.json',encoding='utf-8')); need=all(k in m for k in ('seed','frame_content_hash','population_hash','report_id','strata_weights','gold_allocation','confirmation_allocation','allowed_verdicts')); oc=pathlib.Path('.planning/phases/135-precision-certificate-confidence-bands/cert01_oc_table.md').read_text(encoding='utf-8'); sys.exit(0 if (need and '0.85' in oc) else 1)" && python scripts/check_atlas_masking.py --scan-repo --strict && echo OK` | ❌ W0 (tracked) | ⬜ pending |
| 135-09-02 | 09 | 6 | CERT-01 | T-135-09-02 | deck + blinded demoted+retained diagnostic sample (Codex #14); graders blind; deck_manifest_hash bound back to the manifest; adapter matches reused E1 fns | unit (presence-gated) | `python -c "import os,subprocess,sys; p='same_work_spike/probe/scripts/cert01_frame_adapter.py'; sys.exit(subprocess.call([sys.executable,'-m','pytest','tests/test_cert01_harness_adapter.py','-q']) if os.path.exists(p) else 0)"` | ❌ W0 (only if adapter written) | ⬜ pending |
| 135-09-03 | 09 | 6 | CERT-01 | T-135-09-05 | BLOCKING human checkpoint (Codex #3) + mechanical validator: deck size ~200-250, deck/report hash match, non-empty verdict vocab, uid∈deck, grader attribution, NO grader-visible demotion field | checkpoint:human-verify + unit | `python scripts/verify_cert01_grading.py && pytest tests/test_cert01_grading_validator.py -q && echo OK` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

New test files / fixtures / tracked artifacts / scripts the plans introduce (must exist / be scaffolded before the tasks that depend on them run):

- [ ] `tests/test_discovery_band_labels.py` (135-01) — drift guard, data-driven precision copy (incl. CI-omission + fail-closed-on-partial), rendered-output word gate, tier_a no-bare-number, SC#1 `serialize_banded_claim` inseparability, D-18 `is_default_eligible` table, `band_measurement_status` data-driven, D-11/D-12 constants.
- [ ] `docs/specs/discovery-cert01-protocol.md` (135-03) — the TRACKED, masking-clean pre-registered CERT-01 protocol (replaces the earlier gitignored spike-tree doc — Codex #2).
- [ ] `tests/render_smoke/test_help_methods_render_smoke.py` (135-02) — modeled on `test_atlas_render_smoke.py`; BAND-05 render (all 7 bands, full field set sourced per the field_sourcing table, per-band anchors), body-card independent gating, three-state discovery_methods_noindex (pre-release/REL-01/off), HE RTL, no-"certified" regex; patches `web.pages.help.discovery_available` + `web.main.discovery_methods_noindex`.
- [ ] `tests/test_discovery_v2_bake.py` (135-06) — new file (extends `tests/test_discovery_build.py`); fixtures for merge pair / D-14 flip / overlapping merged-twin (no demotion) / drop / low-coverage Lever-1 / demotion cluster / earliest-low-coverage-later-shipped / multi-cause / orphan-shipped / unknown-date / within-100y tie / coverage-gate HALT / routing-audit replayability / FAIL-branch reband.
- [ ] `tests/fixtures/discovery/` — v2-analog fixture rows carrying band/merge/demotion/routing_status/routing_audit/band_precision-registry-column cases for 135-06.
- [ ] `tests/test_discovery_ids.py` (135-05) — EXISTS; extend with golden-digest / enum-membership assertions for `routing_reason` gaining `later_shared_text` and the v2 band key.
- [ ] `.planning/phases/135-precision-certificate-confidence-bands/cert01_freeze_manifest.json` + `cert01_oc_table.md` (135-09) — TRACKED, masking-clean freeze manifest (report_id/population_hash/allocations) + OC table (Codex #2).
- [ ] `scripts/verify_cert01_grading.py` + `tests/test_cert01_grading_validator.py` (135-09) — the mechanical grading-STARTED validator (six forge-resistant checks) + its load-bearing tests (Codex #3).
- [ ] `tests/test_cert01_harness_adapter.py` (135-09) — ONLY if `same_work_spike/probe/scripts/cert01_frame_adapter.py` is written; presence-gated (documented pure-CLI path otherwise).

Existing infrastructure covers everything else (`tests/test_discovery_ids.py`, `tests/test_discovery_bands.py`, `scripts/verify_discovery_sidecar.py`, `scripts/check_atlas_masking.py`).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Production v2 asset deploy (135-08 Task 1) | CERT-01 | Human-approved, asset-first, deploy-ONCE production checkpoint (D-04). The go/no-go approval, the on-box staged masking re-scan against the uploaded bytes, and the ATOMIC-MANIFEST-SWAP judgment (candidate → prev → swap) are a blocking human checkpoint — no autonomous path. *(The DEPLOY-LOG hash comparison to the 135-07 frame hashes + the candidate/prev/atomic-manifest evidence IS automated in 135-08-01.)* | Approve per `docs/specs/discovery-deploy.md` §2; confirm the DEPLOY-LOG records the matched 135-07 content_hash/frame_content_hash, the candidate→prev→atomic manifest swap, and a single (never v1-then-v2) deploy. |
| CERT-01 genuine expert grading judgment (135-09 Task 3) | CERT-01 | Only a human can attest that the recorded verdict(s) are GENUINE owner/expert judgments (not an autonomous fill) — a blocking human checkpoint (Codex #3). *(The forge-resistant structural sub-checks — deck size, deck_manifest_hash/report_id match against the TRACKED manifest, non-empty verdict vocabulary, uid∈frozen-deck membership, grader attribution, and NO grader-visible demotion field — ARE automated by `scripts/verify_cert01_grading.py` in 135-09-03. The freeze-before-draw ordering is now proven by the MANIFEST HASH, not a git-timestamp inspection — Codex #2, so it too is automated.)* | Confirm an owner/expert recorded ≥1 attributed verdict AND `python scripts/verify_cert01_grading.py` exits 0; the freeze manifest (report_id) precedes the deck by hash binding (deck_manifest_hash), not timestamp. |

All other phase behaviors have automated verification.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies (every command is cross-platform Python / pytest / script — no POSIX `test`/`[ -f ]`/`if`/`!`, Codex #17)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (Wave 6 / 135-09: Task 1 & Task 3 are failing-capable automated checks; Task 2 is a presence-gated pytest run — ≥2 of 3 hold unconditionally; Task 3, though a human checkpoint, carries a mechanical validator + its test)
- [x] Wave 0 covers all MISSING references (incl. the tracked CERT-01 artifacts + the grading validator)
- [x] No watch-mode flags
- [x] Feedback latency < 15s
- [x] `nyquist_compliant: true` set in frontmatter (`wave_0_complete` stays false — Wave 0 scaffolds are authored by the executors)

**Approval:** pending
