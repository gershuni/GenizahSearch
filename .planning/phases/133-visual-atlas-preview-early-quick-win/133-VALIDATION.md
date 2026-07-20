---
phase: 133
slug: visual-atlas-preview-early-quick-win
status: planned
nyquist_compliant: true
wave_0_complete: false
created: 2026-07-20
updated: 2026-07-20
---

# Phase 133 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini / conftest.py (GUI/render-smoke split; run with `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`) |
| **Quick run command** | `python -m pytest tests/test_atlas_*.py -q` |
| **Render-smoke command** | `python -m pytest tests/render_smoke/test_atlas_render_smoke.py tests/render_smoke/test_home_teaser_render_smoke.py -m render_smoke -q` |
| **Masking gate** | `python scripts/check_atlas_masking.py --scan-repo` (exit 0) + `--scan-asset web/static/atlas/atlas-v1.bin` (exit 0) |
| **Full suite command** | `python -m pytest tests/ -q` |
| **Estimated runtime** | ~60 seconds (targeted) / full suite deferred to CI |

---

## Sampling Rate

- **After every task commit:** `python -m pytest tests/test_atlas_*.py -q`
- **After every plan wave:** targeted atlas suite + render-smoke + `python scripts/check_atlas_masking.py --scan-repo`
- **Before `/gsd:verify-work`:** targeted atlas suite + render-smoke + masking scan over the REAL built asset + rendered `/atlas` and `/` output must all be green
- **Max feedback latency:** 60 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 01-T1 | 133-01 | 1 | ATLAS-01 | T-133-01/07 | Masking scan catches injected known-bad pattern; fails safe with no patterns | unit | `python -m pytest tests/test_atlas_masking_scan.py -q` | ❌ W0 | ⬜ pending |
| 01-T2 | 133-01 | 1 | ATLAS-01 | T-133-08 | Working-tree M-source leak scrubbed; committed-repo scan exits 0 | script | `python scripts/check_atlas_masking.py --scan-repo` | ❌ W0 | ⬜ pending |
| 01-T3 | 133-01 | 1 | ATLAS-01 | T-133-07 | Pattern file + baked-asset dir gitignored; `${{ secrets.* }}` CI recipe designed (no repo precedent) | cli | `git check-ignore web/static/atlas/atlas-v1.bin` | n/a | ⬜ pending |
| 02-T1 | 133-02 | 1 | ATLAS-01 | T-133-01 | Bake places >=62,414 stars; no discovery fields; bilingual labels | script | `python scripts/build_atlas_asset.py --smoke 200 --report` | ❌ W0 | ⬜ pending |
| 02-T2 | 133-02 | 1 | ATLAS-01 | T-133-02/05 | Typed-array+Brotli asset <=6MB; versioned; sys_id precise | script | `python scripts/build_atlas_asset.py --smoke 200 --out-dir web/static/atlas` | ❌ W0 | ⬜ pending |
| 02-T3 | 133-02 | 1 | ATLAS-01 | T-133-01 | Bake invariants (no overlay, byte budget, sys_id, determinism) | unit | `python -m pytest tests/test_atlas_bake.py -q` | ❌ W0 | ⬜ pending |
| 03-T1 | 133-03 | 2 | ATLAS-01 | T-133-03/06/09 | /atlas gates on flag+asset in-handler (`ATLAS_PREVIEW_ENABLED` + `os.path.exists(ATLAS_BIN_PATH)`); noindex; clean-hide; env-var docs synced to CLAUDE.md + DEVELOPER_GUIDE.md | source/unit | `python -m pytest tests/test_atlas_flag_gating.py -q` | ❌ W0 | ⬜ pending |
| 03-T2 | 133-03 | 2 | ATLAS-01 | T-133-04 | Brotli route: Content-Encoding br + fallback; hardcoded path; 404 if absent | source/unit | `python -m pytest tests/test_atlas_flag_gating.py -q` | ❌ W0 | ⬜ pending |
| 03-T3 | 133-03 | 2 | ATLAS-01 | T-133-09 | Chrome bilingual; CLS-reserved canvas; flag-OFF clean-hide + flag-ON/asset-absent page clean-hide (NOT renderer) + data-route 404 | unit | `python -m pytest tests/test_atlas_flag_gating.py -q` | ❌ W0 | ⬜ pending |
| 04-T1 | 133-04 | 3 | ATLAS-01 | T-133-02 | Fetch+decode payload; no overlay; domain-colored galaxy | render_smoke | `python -m pytest tests/render_smoke/test_atlas_render_smoke.py -m render_smoke -q` | ❌ W0 | ⬜ pending |
| 04-T2 | 133-04 | 3 | ATLAS-01 | T-133-10/11 | Interactions + reduced-motion intro + /browse click-through | render_smoke | `python -m pytest tests/render_smoke/test_atlas_render_smoke.py -m render_smoke -q` | ❌ W0 | ⬜ pending |
| 04-T3 | 133-04 | 3 | ATLAS-01 | T-133-01 | Live render: chrome + CLS canvas + EN/HE + RTL | render_smoke | `python -m pytest tests/render_smoke/test_atlas_render_smoke.py -m render_smoke -q` | ❌ W0 | ⬜ pending |
| 05-T1 | 133-05 | 3 | ATLAS-01 | T-133-06/03 | Flag-gated claim-free teaser -> /atlas | source | `python -m pytest tests/render_smoke/test_home_teaser_render_smoke.py -m render_smoke -q` | ❌ W0 | ⬜ pending |
| 05-T2 | 133-05 | 3 | ATLAS-01 | T-133-12 | Teaser render smoke: ON->card+link+claim-free; OFF->absent | render_smoke | `python -m pytest tests/render_smoke/test_home_teaser_render_smoke.py -m render_smoke -q` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Each plan front-loads (or co-creates) its own test file — there is no separate Wave 0 plan; the scaffolds are created inside the plan that owns them:

- [ ] `scripts/check_atlas_masking.py` + `tests/test_atlas_masking_scan.py` — created in plan 133-01 Task 1 (sanity-injection self-test; fail-safe with no patterns) — D-07 → forerunner of DATA-05
- [ ] `scripts/build_atlas_asset.py` + `tests/test_atlas_bake.py` — created in plan 133-02 (`--smoke` fixture mode so tests need no research DB); node-inclusion logic, no-discovery-fields, byte budget, sys_id round-trip, determinism
- [ ] `tests/test_atlas_flag_gating.py` — created in plan 133-03 Task 3 (flag-OFF clean-hide + flag-ON/asset-absent page clean-hide + data-route 404 + in-handler flag+asset (`os.path.exists(ATLAS_BIN_PATH)`) reference)
- [ ] `tests/render_smoke/test_atlas_render_smoke.py` — created in plan 133-04 Task 3 (modeled on `tests/render_smoke/test_joins_lab_render_smoke.py`)
- [ ] `tests/render_smoke/test_home_teaser_render_smoke.py` — created in plan 133-05 Task 2
- [ ] Framework install: none — pytest + `nicegui.testing.User` harness already present

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Canvas 2D render fidelity (bloom-in intro, zoom/pan, focus-constellation, color toggle, library filter) | ATLAS-01 SC#1/#4 | Headless pytest cannot exercise the interactive Canvas renderer; needs a live client | Open `/atlas` with the flag ON in a live web session; verify render, reduced-motion skip, EN/HE toggle + RTL chrome, click-through to `/browse`, CLS-safe (reserved canvas) |
| Homepage teaser card render + link | ATLAS-01 SC#6 | Live render smoke only | Open `/` with the flag ON; verify CLS-safe static card, EN/HE + RTL, links to `/atlas` (which is noindex) |
| Node count >= 62,414 against the real research DB | ATLAS-01 SC#1 / D-09 | Requires the 2.9 GB gitignored research DB (not in CI) | `python scripts/build_atlas_asset.py <research-db> --report` prints placed-star count >= 62,414 |
| Flag-ON/asset-absent live clean-hide (asset not yet scp'd) | ATLAS-01 SC#2 / D-13 | The automated Task 3 guard covers the code branch; a live check confirms the deployed-but-asset-missing window renders the "temporarily unavailable" card with zero console/network errors | With `ATLAS_PREVIEW_ENABLED` ON in prod BEFORE the asset is scp'd, open `/atlas`: expect the clean "temporarily unavailable" card (no beta chrome over a 404ing `/atlas-data/atlas-v1.bin` fetch) |
| Masking scan over the REAL built asset + rendered output | ATLAS-01 SC#3 | Requires the real built asset + a rendered-page capture | `python scripts/check_atlas_masking.py --scan-asset web/static/atlas/atlas-v1.bin` + scan the rendered `/atlas` and `/` HTML → exit 0 |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (each plan co-creates its test scaffold)
- [x] No watch-mode flags
- [x] Feedback latency < 60s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** planner-approved 2026-07-20 (revised 2026-07-20 — flag-ON/asset-absent page clean-hide gap closed in 03-T1/T3)
