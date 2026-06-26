---
phase: 125
slug: core-engines
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-26
---

# Phase 125 — Validation Strategy

> Per-phase validation contract. Phase 125 = SEED-011 composition dedup (125a, behavior-preserving)
> + extraction of SearchEngine / LabSettings / LabEngine to `shared/` (125b–d). Validation is
> dominated by identity/facade (CORE-10..13, GUARD-04), back-edge (GUARD-01), source-scan retarget
> (GUARD-03), and zero-behavior-change (GUARD-02), PLUS a SEED-011 dedup behavior + invocation-count
> check. **The PREP-01 BOM removal is ALREADY DONE** (genizah_core.py `29d51f4a` + shared/responsa.py
> in the pre-125 Codex audit), so 125a's first planned commit is complete; the 7 BOM-victim tests are
> already green.

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing) |
| **Config file** | `tests/conftest.py` |
| **Quick run command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_no_back_edges_core.py tests/test_comp_corpus_scope.py tests/test_lab_composition_chunk_hits.py tests/test_local_lab_invalidation.py tests/test_local_post_dedup_merge.py tests/test_phase_97_invariants.py tests/test_audit_2026_06_23_guards.py -q -p no:cacheprovider` |
| **Full suite command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -q -m "not gui and not render_smoke"` (NO `-n auto`) + separate `-m gui` / `-m render_smoke` slices |
| **Estimated runtime** | quick ~30–90s; bulk ~9 min |

## Source-Integrity Gate (adopted from the pre-125 Codex audit — run BEFORE pytest/review on every extraction commit)

1. **No BOM / UTF-8 / LF** on every touched `.py` (`head -c3 | xxd`; periodic 611-file scan).
2. **`git diff --check HEAD`** clean on the new changes.
3. **Runtime facade identity** — `genizah_core.X is shared.Y.X` for every moved name (orchestrator runs; Codex sandbox has no Python).
4. **AST method/symbol-completeness** — moved classes' method sets + modules' function sets match base `08c43bea`.
5. **base-vs-HEAD NAME-level test diff + facade-name diff** (Phase-124 lesson — never trust the executor's failure count).

## Sampling Rate

- **After every task commit:** source-integrity gate (1–4) + the quick run command.
- **After every wave:** bulk suite + gui/render_smoke slices.
- **Before `/gsd:verify-work`:** full suite green; per-file ruff clean.

## Per-Task / Per-Req Verification Map

| Req ID | Behavior | Test Type | Automated Command | Status |
|--------|----------|-----------|-------------------|--------|
| PREP-01 (BOM) | genizah_core.py + shared/responsa.py carry no BOM | structural | `python -c "import subprocess;assert all(open(f,'rb').read(3)!=b'\xef\xbb\xbf' for f in subprocess.check_output(['git','ls-files','*.py'],text=True).split())"` | ✅ DONE (audit) |
| PREP-01 (dedup) | composition dedup tests green | unit | quick run command | ✅ green post-BOM-fix |
| PREP-01 (SEED-011) | per-chunk plan built once, not N×2 (Genizah + LAB) | unit (mock invocation-count) | `pytest tests/test_seed011_composition_dedup.py -q` | ❌ Wave 0 (new) |
| CORE-10 | `genizah_core.SearchEngine is shared.search_engine.SearchEngine` + 3 hazards intact | identity + structural | `pytest tests/test_no_back_edges_core.py -k search_engine -q` | ❌ Wave 0 |
| CORE-11 | `genizah_core.LabSettings is shared.lab_settings.LabSettings` | identity | `pytest tests/test_no_back_edges_core.py -k lab_settings -q` | ❌ Wave 0 |
| CORE-12 | `genizah_core.LabEngine is shared.lab_engine.LabEngine`; CR-01/CR-02 + `_lab_weights_hash_override` mirror intact | identity + structural | `pytest tests/test_no_back_edges_core.py tests/test_local_lab_invalidation.py -q` | ❌ Wave 0 / ✅ |
| CORE-13 | `_my_library_tab_ref` injected gate on BOTH engines; no shared/→desktop import | GUARD-01 | `pytest tests/test_no_back_edges_core.py -q` | ✅ (registry → 13) |
| GUARD-02 | zero new failures in any search mode (keyword/Responsa/composition/parallels/Local/ALL) | suite | full suite + base-vs-HEAD name-level diff | ✅ |
| GUARD-03 | source-scan tests pass after the engine move (retargeted) | suite | `pytest tests/test_audit_2026_06_23_guards.py tests/test_local_lab_invalidation.py tests/test_local_post_dedup_merge.py tests/test_phase_97_invariants.py tests/test_lab_composition_chunk_hits.py -q` | ✅ (retargeted) |
| GUARD-04 | facade preserves all engine names (incl. content_search gate names + 6 `_LAST_RESPONSA_DOWNGRADE` channel names) | identity + name-diff | base-vs-HEAD facade-name diff + identity asserts | N/A (gate) |

*Status: ⬜ pending · ✅ green · ❌ red/Wave-0 · ⚠️ flaky*

## Wave 0 Requirements

- [ ] `tests/test_seed011_composition_dedup.py` — NEW: assert the per-chunk plan (query/regex/weak/fingerprint) is built ONCE per chunk, not 2× under `corpus_scope='all'` and in LAB composition (mock `build_tantivy_query` / the prep call, assert invocation count). Covers `search_composition_logic` (two-query `ChunkPlan`: `genizah_query_str` + `local_query_str` for the SEED-006 diacritic-fold) and `lab_composition_search`.
- [ ] `tests/test_no_back_edges_core.py` — grow `EXTRACTED_MODULES` registry 10 → 13 (search_engine, lab_settings, lab_engine) + add identity/standalone tests for each; add the engine facade names.
- [ ] GUARD-03 retargets land in the SAME commit as each engine move: `test_audit_2026_06_23_guards.py`, `test_local_lab_invalidation.py`, `test_local_post_dedup_merge.py`, `test_phase_97_invariants.py`, `test_lab_composition_chunk_hits.py` (any that source-scan a moved engine method → read the new shared module).

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Instructions |
|----------|-------------|------------|--------------|
| Codex PLAN pre-flight (plan↔code drift) | all | cross-AI, before execute | `codex exec -s read-only "$(cat brief)" < /dev/null` over PLAN + RESEARCH vs live code; must clear |
| Codex CODE review 3-round + facade/name diff | GUARD-02/04 | cross-AI, after execute | per the adopted drill; converge to APPROVE |

## Validation Sign-Off

- [ ] All tasks have automated verify or Wave 0 deps
- [ ] Source-integrity gate wired into per-commit sampling
- [ ] Wave 0 covers SEED-011 dedup test + registry growth + GUARD-03 retargets
- [ ] Feedback latency acceptable (quick run < 90s)
- [ ] `nyquist_compliant: true` set when Wave 0 lands

**Approval:** pending
