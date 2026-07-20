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
> Revised 2026-07-20 (Codex pre-flight rework): 6 plans / 5 waves; asset moved off `/static` to repo-root `atlas_data/`; masking scan is multi-surface + recursive; node-count is EXACT set equality; a frozen binary schema + golden cross-language decode; a dedicated pinned atlas-bake CI job; a final production deploy checkpoint (133-06).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini_options in pyproject.toml + conftest.py (gui / render_smoke / atlas_bake splits; run with `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`) |
| **Quick run command** | `python -m pytest tests/test_atlas_masking_scan.py tests/test_atlas_flag_gating.py -q` |
| **Atlas-bake command** | `python -m pytest tests/atlas_bake -m atlas_bake -q` (pinned deps from `requirements-atlas-bake.txt`; Node on PATH for the golden JS-decode + DOM-XSS test) |
| **Render-smoke command** | `python -m pytest tests/render_smoke/test_atlas_render_smoke.py tests/render_smoke/test_home_teaser_render_smoke.py -m render_smoke -q` |
| **Masking gate** | `python scripts/check_atlas_masking.py --scan-repo` (exit 0) + `--scan-asset atlas_data/` (recursive; exit 0) + `--scan-asset <captured-rendered-HTML>` (exit 0) |
| **Full suite command** | `python -m pytest tests/ -q` |
| **Estimated runtime** | ~70 seconds (targeted) / full suite deferred to CI |

---

## Sampling Rate

- **After every task commit:** the plan's own `<automated>` command (targeted).
- **After every plan wave:** targeted atlas suite + (from wave 2) atlas-bake suite + (from wave 4) render-smoke + `python scripts/check_atlas_masking.py --scan-repo`.
- **Before `/gsd:verify-work`:** targeted atlas suite + atlas-bake suite + render-smoke + masking scan over the REAL built asset dir (`atlas_data/`) AND the captured rendered `/atlas` and `/` HTML must all be green.
- **Max feedback latency:** 70 seconds.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 01-T1 | 133-01 | 1 | ATLAS-01 | T-133-01/07/13 | Ignore rules first; multi-surface scan_repo (HEAD/index+worktree+untracked) + recursive scan_asset (text+bytes+normalized/encoded); never-echo; fails safe with no patterns | unit | `python -m pytest tests/test_atlas_masking_scan.py -q` | ❌ W0 | ⬜ pending |
| 01-T2 | 133-01 | 1 | ATLAS-01 | T-133-08 | Working-tree M-source leak scrubbed (unrelated edits preserved); repo scan exits 0 across all surfaces | script | `python scripts/check_atlas_masking.py --scan-repo` | ❌ W0 | ⬜ pending |
| 01-T3 | 133-01 | 1 | ATLAS-01 | T-133-07 | Pre-commit green gate + `${{ secrets.* }}` CI recipe designed + explicit-path/hunk commit discipline | script | `python scripts/check_atlas_masking.py --scan-repo` | n/a | ⬜ pending |
| 02-T1 | 133-02 | 2 | ATLAS-01 | T-133-01/05 | Frozen binary schema; bake proves EXACT eligible==placed; no discovery fields; bilingual labels; nargs db_path | script | `python scripts/build_atlas_asset.py --smoke 200 --report` | ❌ W0 | ⬜ pending |
| 02-T2 | 133-02 | 2 | ATLAS-01 | T-133-02/05 | Schema-conformant typed-array+Brotli asset <=6MB; content-hashed filename; sys_id precise; manifest eligible/placed/missing/extra | script | `python scripts/build_atlas_asset.py --smoke 200 --out-dir atlas_data` | ❌ W0 | ⬜ pending |
| 02-T3 | 133-02 | 2 | ATLAS-01 | T-133-01/SC | Bake invariants (exact set, byte budget, sys_id, determinism, content-hash invalidation, golden PYTHON per-field decode) + pinned CI bake job | unit | `python -m pytest tests/atlas_bake/test_atlas_bake.py -q` | ❌ W0 | ⬜ pending |
| 03-T1 | 133-03 | 3 | ATLAS-01 | T-133-03/06/09/13 | Flag + web/atlas_assets authoritative loader; /atlas + nav gate on `atlas_preview_available()`; noindex; clean-hide; env-var + CODE_INDEX docs | source/unit | `python -m pytest tests/test_atlas_flag_gating.py -q` | ❌ W0 | ⬜ pending |
| 03-T2 | 133-03 | 3 | ATLAS-01 | T-133-04/13/14 | Data routes off `/static`; whitelist content-hashed name; Accept-Encoding q-value negotiation (br/plain/406); manifest short-cache, asset immutable | source/unit | `python -m pytest tests/test_atlas_flag_gating.py -q` | ❌ W0 | ⬜ pending |
| 03-T3 | 133-03 | 3 | ATLAS-01 | T-133-09/01 | Chrome bilingual + CLS canvas; complete HE string set; three-surface predicate + flag-OFF + flag-ON/asset-not-loaded clean-hide + data 404 + Brotli response tests + HE translated values | unit | `python -m pytest tests/test_atlas_flag_gating.py -q` | ❌ W0 | ⬜ pending |
| 04-T1 | 133-04 | 4 | ATLAS-01 | T-133-02/16 | Decoder JS implements frozen schema; fetch manifest+content-hashed asset; draw galaxy; no overlay | source | `python -m pytest tests/render_smoke/test_atlas_render_smoke.py -m render_smoke -q` | ❌ W0 | ⬜ pending |
| 04-T2 | 133-04 | 4 | ATLAS-01 | T-133-10/11/15 | Interactions + reduced-motion intro + /browse click-through; ALL catalogue DOM via createElement/textContent (no innerHTML) | source | `python -m pytest tests/render_smoke/test_atlas_render_smoke.py -m render_smoke -q` | ❌ W0 | ⬜ pending |
| 04-T3 | 133-04 | 4 | ATLAS-01 | T-133-01/15/16 | Server render smoke (chrome/CLS/EN-HE-RTL) + Node golden JS decode (==Python) + Node DOM-XSS neutralization + static no-innerHTML guard | render_smoke + atlas_bake | `python -m pytest tests/render_smoke/test_atlas_render_smoke.py -m render_smoke -q` then `python -m pytest tests/atlas_bake/test_atlas_golden_js.py -q` | ❌ W0 | ⬜ pending |
| 05-T1 | 133-05 | 4 | ATLAS-01 | T-133-06/03 | Teaser gated on shared `atlas_preview_available()` (fourth surface) -> /atlas; claim-free | source | `python -m pytest tests/render_smoke/test_home_teaser_render_smoke.py -m render_smoke -q` | ❌ W0 | ⬜ pending |
| 05-T2 | 133-05 | 4 | ATLAS-01 | T-133-12 | Teaser render smoke: available->card+link+claim-free+HE values; OFF & asset-not-loaded->absent | render_smoke | `python -m pytest tests/render_smoke/test_home_teaser_render_smoke.py -m render_smoke -q` | ❌ W0 | ⬜ pending |
| 06-T1 | 133-06 | 5 | ATLAS-01 | T-133-01 | Real bake (exact eligible==placed) + content-hash + recursive masking scan over atlas_data/ AND captured rendered /atlas + / HTML | script | `python scripts/check_atlas_masking.py --scan-asset atlas_data/` | ❌ W0 | ⬜ pending |
| 06-T2 | 133-06 | 5 | ATLAS-01 | T-133-13/17 | Asset-first prod upload (outside static root) -> code deploy -> flag set -> restart; manifest route serves the content-hashed asset live | checkpoint:human-verify | (human) manifest route 200 + asset-first order | n/a | ⬜ pending |
| 06-T3 | 133-06 | 5 | ATLAS-01 | T-133-01/13/18 | Prod smoke: render + Brotli negotiation + noindex + EN/HE + teaser + LIVE masking scan exit 0 + rollback drill | checkpoint:human-verify | (human) live smoke + `check_atlas_masking.py --scan-asset <live-capture>` | n/a | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Each plan front-loads (or co-creates) its own test file — there is no separate Wave 0 plan; the scaffolds are created inside the plan that owns them:

- [ ] `.gitignore` entries (`/.masking_patterns`, `/atlas_data/`) + `scripts/check_atlas_masking.py` (multi-surface scan_repo + recursive scan_asset, never-echo, fail-safe) + `tests/test_atlas_masking_scan.py` (sanity-injection across surfaces) — plan 133-01 Task 1 — D-07 → forerunner of DATA-05
- [ ] `docs/specs/atlas-asset-schema-v1.md` (FROZEN binary contract) + `scripts/build_atlas_asset.py` — plan 133-02 Tasks 1-2
- [ ] `tests/atlas_bake/test_atlas_bake.py` (bake invariants incl. golden Python decode) + `tests/fixtures/atlas/golden-v1.bin(.br)` + `golden-v1-expected.json` + `requirements-atlas-bake.txt` (pinned) + `atlas_bake` marker in pyproject.toml + conftest path injection + `atlas-bake-tests` CI job (with setup-node) + `and not atlas_bake` on the default tests job — plan 133-02 Task 3
- [ ] `web/atlas_assets.py` (authoritative asset-state loader + `atlas_preview_available()`) + `tests/test_atlas_flag_gating.py` (three-surface predicate + flag-OFF + flag-ON/asset-not-loaded clean-hide + data-route 404 + Brotli q-negotiation + HE translated values) — plan 133-03 Tasks 1-3
- [ ] `web/static/js/atlas_decode.js` (frozen-schema decoder + XSS-safe DOM builders) + `tests/render_smoke/test_atlas_render_smoke.py` (server render only) + `tests/atlas_bake/test_atlas_golden_js.py` (Node golden JS decode == Python + DOM-XSS neutralization + static no-innerHTML guard) — plan 133-04
- [ ] `tests/render_smoke/test_home_teaser_render_smoke.py` — plan 133-05 Task 2
- [ ] `scripts/capture_atlas_html.py` (ASGI capture of rendered /atlas + / for the masking scan) — plan 133-06 Task 1
- [ ] Framework install: pytest + `nicegui.testing.User` harness already present; `requirements-atlas-bake.txt` (networkx/python-louvain/Brotli, build-only) installed in the dedicated `atlas-bake-tests` CI job; Node available in that job for the golden JS-decode test

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Canvas 2D render fidelity + INTERACTIONS (bloom-in intro, zoom/pan, focus-constellation, color toggle, library filter, tooltips, click-through) | ATLAS-01 SC#1/#4 | Headless pytest cannot exercise the interactive Canvas renderer; the NiceGUI User harness only validates the SERVER-side component tree, NOT client JS/Canvas. The fetch/decode is proven by the automated Node golden test (04-T3), but visual/interaction FIDELITY is manual | Open `/atlas` with the flag ON in a live web session; verify render, reduced-motion skip, EN/HE toggle + RTL chrome, zoom/pan/focus/color-toggle/library-filter, click-through to `/browse`, CLS-safe (reserved canvas) |
| Binary decode correctness across JS + Python | ATLAS-01 SC#1 | Automated via the golden fixture (NOT manual) | `python -m pytest tests/atlas_bake/test_atlas_golden_js.py -q` (JS decode == `golden-v1-expected.json` == Python decode in `tests/atlas_bake/test_atlas_bake.py`) |
| Homepage teaser card render + link | ATLAS-01 SC#6 | Live render smoke only for full fidelity | Open `/` with the flag ON + asset loaded; verify CLS-safe static card, EN/HE + RTL, links to `/atlas` (noindex) |
| Node-set completeness against the real research DB (EXACT eligible==placed, ~62,645; floor >= 62,414) | ATLAS-01 SC#1 / D-09 | Requires the 2.9 GB gitignored research DB (not in CI) | `python scripts/build_atlas_asset.py <research-db> --report` prints eligible_count==placed_count, missing=0, extra=0, placed >= 62,414 (plan 133-06 Task 1) |
| Live Brotli Content-Encoding negotiation | ATLAS-01 SC#4/#5 | Requires the deployed prod server (transport-level headers) | `curl -D- -H 'Accept-Encoding: br' https://genizahsearch.com/atlas-data/<asset_basename>.bin` -> `Content-Encoding: br`; without the header -> plain bytes, no `Content-Encoding` (plan 133-06 Task 3) |
| Masking scan over the REAL built asset + LOCAL and LIVE rendered output | ATLAS-01 SC#3 | Requires the real built asset + a rendered-page capture (local via ASGI, live via curl) | `python scripts/check_atlas_masking.py --scan-asset atlas_data/` + `--scan-asset <captured /atlas + / HTML>` → exit 0 (plan 133-06 Tasks 1 + 3) |
| Production go-live + rollback drill | ATLAS-01 SC#5 | Prod-touching; asset-first upload + flag flip + restart + rollback need human confirmation | Plan 133-06 Tasks 2-3: asset scp'd first (outside static root) → code deploy → flag set → restart → live render/Brotli/noindex/EN-HE/teaser smoke → flag-OFF rollback drill (clean-hide, nav/teaser gone, data route 404, rest of app intact) |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or a human-checkpoint (133-06 prod tasks are checkpoint:human-verify by necessity — they touch production)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (the only human checkpoints are the two prod-deploy tasks in 133-06, each preceded by 06-T1's automated gate)
- [x] Wave 0 covers all MISSING references (each plan co-creates its test scaffold; the golden fixture + decoder module + capture helper are enumerated)
- [x] No watch-mode flags
- [x] Feedback latency < 70s
- [x] `nyquist_compliant: true` set in frontmatter
- [x] MEDIUM-2 corrected: fetch/decode + Canvas interactions are NOT labeled render-smoke-covered — decode is Node-golden-covered, interactions are manual UAT

**Approval:** planner-approved 2026-07-20 (revised 2026-07-20 — Codex pre-flight rework: waves restructured to 5, 133-06 added, asset off `/static`, exact node-set, frozen schema + golden cross-language decode, DOM-XSS safety, Brotli q-negotiation, four-surface predicate, pinned atlas-bake CI)
