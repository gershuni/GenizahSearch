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
> Revised 2026-07-20 (Codex confirmation pass, round 2): 133-01 is ONE atomic precondition task (single terminal commit gated on scrub + clean 3-pass scan — the phase's FIRST commit, HIGH-3); masking gate now also scans the LIVE headless-browser CLIENT DOM (HIGH-4); a parametrized four-surface integration test (page/data/nav/teaser) in 133-06 (MEDIUM-6); Brotli negotiation parses br/identity/* q-values with a reachable 406 and the manifest is no-cache + ETag (MEDIUM-3 + stale-manifest); sys_id is BigUint64-only, no fallback (NEW LOW); deploy/release docs added to 133-06 (NEW LOW); 04-T1/04-T2 relabeled off render-smoke (MEDIUM-2); CI secret path = `${GITHUB_WORKSPACE}/.masking_patterns` (NEW MEDIUM).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini_options in pyproject.toml + conftest.py (gui / render_smoke / atlas_bake splits; run with `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`) |
| **Quick run command** | `python -m pytest tests/test_atlas_masking_scan.py tests/test_atlas_flag_gating.py -q` |
| **Atlas-bake command** | `python -m pytest tests/atlas_bake -m atlas_bake -q` (pinned deps from `requirements-atlas-bake.txt`; Node on PATH for the golden JS-decode + DOM-XSS test) |
| **Render-smoke command** | `python -m pytest tests/render_smoke/test_atlas_render_smoke.py tests/render_smoke/test_home_teaser_render_smoke.py tests/render_smoke/test_atlas_four_surface.py -m render_smoke -q` |
| **Masking gate** | `python scripts/check_atlas_masking.py --scan-repo` (exit 0) + `--scan-asset atlas_data/` (recursive; exit 0) + `--scan-asset <ASGI-captured HTML>` (exit 0) + `--scan-asset <LIVE headless-browser client DOM>` (exit 0, live smoke — HIGH-4) |
| **Full suite command** | `python -m pytest tests/ -q` |
| **Estimated runtime** | ~70 seconds (targeted) / full suite deferred to CI |

---

## Sampling Rate

- **After every task commit:** the plan's own `<automated>` command (targeted).
- **After every plan wave:** targeted atlas suite + (from wave 2) atlas-bake suite + (from wave 4) render-smoke + `python scripts/check_atlas_masking.py --scan-repo`.
- **Before `/gsd:verify-work`:** targeted atlas suite + atlas-bake suite + render-smoke (incl. the four-surface test) + masking scan over the REAL built asset dir (`atlas_data/`) AND the captured rendered `/atlas` and `/` HTML must all be green.
- **Max feedback latency:** 70 seconds.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 01-T1 | 133-01 | 1 | ATLAS-01 | T-133-01/07/08/13 | ONE atomic precondition: ignore rules first; multi-surface scan_repo (HEAD/index+worktree+untracked) + recursive scan_asset (text+bytes+normalized/encoded); never-echo; fails safe with no patterns; M-source scrub (unrelated edits preserved); the SINGLE terminal commit (scanner+gitignore+test only) is the phase's FIRST commit, gated on the green 3-pass scan (HIGH-3) | unit + script | `python -m pytest tests/test_atlas_masking_scan.py -q && python scripts/check_atlas_masking.py --scan-repo` | ❌ W0 | ⬜ pending |
| 02-T1 | 133-02 | 2 | ATLAS-01 | T-133-01/05 | Frozen binary schema (sys_id BigUint64-only, no fallback); bake proves EXACT eligible==placed; no discovery fields; bilingual labels; nargs db_path | script | `python scripts/build_atlas_asset.py --smoke 200 --report` | ❌ W0 | ⬜ pending |
| 02-T2 | 133-02 | 2 | ATLAS-01 | T-133-02/05/19 | Schema-conformant typed-array+Brotli asset <=6MB; content-hashed filename; sys_id BigUint64-only (bake FAILS on non-pure-digit/>=2^64, no fallback); manifest eligible/placed/missing/extra | script | `python scripts/build_atlas_asset.py --smoke 200 --out-dir atlas_data` | ❌ W0 | ⬜ pending |
| 02-T3 | 133-02 | 2 | ATLAS-01 | T-133-01/19/SC | Bake invariants (exact set, byte budget, sys_id precision + invalid-fails-bake, determinism, content-hash invalidation, golden PYTHON per-field decode) + pinned CI bake job | unit | `python -m pytest tests/atlas_bake/test_atlas_bake.py -q` | ❌ W0 | ⬜ pending |
| 03-T1 | 133-03 | 3 | ATLAS-01 | T-133-03/06/09/13 | Flag + web/atlas_assets authoritative loader (plain required, brotli optional); /atlas + nav gate on `atlas_preview_available()`; noindex; clean-hide; env-var + CODE_INDEX docs | source/unit | `python -m pytest tests/test_atlas_flag_gating.py -q` | ❌ W0 | ⬜ pending |
| 03-T2 | 133-03 | 3 | ATLAS-01 | T-133-04/05/13/14 | Data routes off `/static`; whitelist content-hashed name; br/identity/* q-value negotiation (reachable 406); manifest no-cache + ETag + 304, asset immutable | source/unit | `python -m pytest tests/test_atlas_flag_gating.py -q` | ❌ W0 | ⬜ pending |
| 03-T3 | 133-03 | 3 | ATLAS-01 | T-133-09/05/14/01 | Chrome bilingual + CLS canvas; complete HE string set; three-surface predicate + flag-OFF + flag-ON/asset-not-loaded clean-hide + data 404 + br/identity/* negotiation + reachable 406 + manifest no-cache+ETag+304 + stale-manifest transition + HE translated values | unit | `python -m pytest tests/test_atlas_flag_gating.py -q` | ❌ W0 | ⬜ pending |
| 04-T1 | 133-04 | 4 | ATLAS-01 | T-133-02/16 | Decoder JS implements frozen schema (sys_id BigUint64-only); fetch manifest+content-hashed asset; draw galaxy; no overlay. Decode FIDELITY = Node golden (04-T3); the galaxy DRAW = manual UAT — NOT server render-smoke (MEDIUM-2) | source (+ Node golden 04-T3, + manual UAT) | `python -c "..."` source scan (133-04 Task-1 verify: fetch-from-route, no `__DATA__`, no overlay) | ❌ W0 | ⬜ pending |
| 04-T2 | 133-04 | 4 | ATLAS-01 | T-133-10/11/15 | Interactions + reduced-motion intro + /browse click-through; ALL catalogue DOM via createElement/textContent (no innerHTML). Interaction FIDELITY = manual UAT — NOT server render-smoke (MEDIUM-2) | source (+ manual UAT) | `python -c "..."` source scan (133-04 Task-2 verify: browse?sys_id + window.location.origin + prefers-reduced-motion + textContent + no catalogue-into-innerHTML) | ❌ W0 | ⬜ pending |
| 04-T3 | 133-04 | 4 | ATLAS-01 | T-133-01/15/16 | Server render smoke (chrome/CLS/EN-HE-RTL) + Node golden JS decode (==Python, sys_id via BigInt(str)) + Node DOM-XSS neutralization + static no-innerHTML guard | render_smoke + atlas_bake | `python -m pytest tests/render_smoke/test_atlas_render_smoke.py -m render_smoke -q` then `python -m pytest tests/atlas_bake/test_atlas_golden_js.py -q` | ❌ W0 | ⬜ pending |
| 05-T1 | 133-05 | 4 | ATLAS-01 | T-133-06/03 | Teaser gated on shared `atlas_preview_available()` (fourth surface) -> /atlas; claim-free | source | `python -m pytest tests/render_smoke/test_home_teaser_render_smoke.py -m render_smoke -q` | ❌ W0 | ⬜ pending |
| 05-T2 | 133-05 | 4 | ATLAS-01 | T-133-12 | Teaser render smoke: available->card+link+claim-free+HE values; OFF & asset-not-loaded->absent | render_smoke | `python -m pytest tests/render_smoke/test_home_teaser_render_smoke.py -m render_smoke -q` | ❌ W0 | ⬜ pending |
| 06-T1 | 133-06 | 5 | ATLAS-01 | T-133-01 | Real bake (exact eligible==placed) + content-hash + capture helper (ASGI + headless-browser client-DOM modes) + recursive masking scan over atlas_data/ AND ASGI-captured /atlas + / HTML | script | `python scripts/check_atlas_masking.py --scan-asset atlas_data/` | ❌ W0 | ⬜ pending |
| 06-T2 | 133-06 | 5 | ATLAS-01 | T-133-20/01 | Parametrized four-surface (page/data/nav/teaser × OFF/asset-missing/ready) integration test (nav + teaser behavioral — MEDIUM-6) + deploy/release docs (DEPLOYMENT_TECHNICAL.md + CHANGELOG.md + README.md — NEW LOW) masking-clean | render_smoke + source | `python -m pytest tests/render_smoke/test_atlas_four_surface.py -m render_smoke -q` | ❌ W0 | ⬜ pending |
| 06-T3 | 133-06 | 5 | ATLAS-01 | T-133-13/17 | Asset-first prod upload (outside static root) -> code deploy -> flag set -> restart; manifest route serves the content-hashed asset live with no-cache + ETag | checkpoint:human-verify | (human) manifest route 200 + no-cache/ETag + asset-first order | n/a | ⬜ pending |
| 06-T4 | 133-06 | 5 | ATLAS-01 | T-133-01/13/18 | Prod smoke: render + Brotli negotiation (br/plain/406) + manifest no-cache+ETag + noindex + EN/HE + teaser + LIVE headless-browser client-DOM masking scan exit 0 (HIGH-4) + rollback drill | checkpoint:human-verify | (human) live smoke + `capture_atlas_html.py --browser-dom` then `check_atlas_masking.py --scan-asset <live-capture>` | n/a | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Each plan front-loads (or co-creates) its own test file — there is no separate Wave 0 plan; the scaffolds are created inside the plan that owns them:

- [ ] `.gitignore` entries (`/.masking_patterns`, `/atlas_data/`) + `scripts/check_atlas_masking.py` (multi-surface scan_repo + recursive scan_asset, never-echo, fail-safe) + `tests/test_atlas_masking_scan.py` (sanity-injection across surfaces) — plan 133-01 (ONE atomic task; the scanner + scrub + green 3-pass scan gate the single terminal commit) — D-07 → forerunner of DATA-05
- [ ] `docs/specs/atlas-asset-schema-v1.md` (FROZEN binary contract; sys_id BigUint64-only) + `scripts/build_atlas_asset.py` — plan 133-02 Tasks 1-2
- [ ] `tests/atlas_bake/test_atlas_bake.py` (bake invariants incl. golden Python decode + sys_id-invalid-fails) + `tests/fixtures/atlas/golden-v1.bin(.br)` + `golden-v1-expected.json` + `requirements-atlas-bake.txt` (pinned) + `atlas_bake` marker in pyproject.toml + conftest path injection + `atlas-bake-tests` CI job (with setup-node) + `and not atlas_bake` on the default tests job — plan 133-02 Task 3
- [ ] `web/atlas_assets.py` (authoritative asset-state loader, plain-required/brotli-optional + `atlas_preview_available()`) + `tests/test_atlas_flag_gating.py` (three-surface predicate + flag-OFF + flag-ON/asset-not-loaded clean-hide + data-route 404 + br/identity/* q-negotiation + reachable 406 + manifest no-cache+ETag+304 + stale-manifest transition + HE translated values) — plan 133-03 Tasks 1-3
- [ ] `web/static/js/atlas_decode.js` (frozen-schema decoder, single BigUint64 sys_id path + XSS-safe DOM builders) + `tests/render_smoke/test_atlas_render_smoke.py` (server render only) + `tests/atlas_bake/test_atlas_golden_js.py` (Node golden JS decode == Python + DOM-XSS neutralization + static no-innerHTML guard) — plan 133-04
- [ ] `tests/render_smoke/test_home_teaser_render_smoke.py` — plan 133-05 Task 2
- [ ] `scripts/capture_atlas_html.py` (ASGI capture + headless-browser client-DOM capture of rendered /atlas + / for the masking scan) + `tests/render_smoke/test_atlas_four_surface.py` (four-surface integration test) + deploy/release doc updates — plan 133-06 Tasks 1-2
- [ ] Framework install: pytest + `nicegui.testing.User` harness already present; `requirements-atlas-bake.txt` (networkx/python-louvain/Brotli, build-only) installed in the dedicated `atlas-bake-tests` CI job; Node available in that job for the golden JS-decode test; Playwright installed ad hoc (dev/ops tool, NOT in any requirements file) for the browser-DOM capture

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Canvas 2D render fidelity + INTERACTIONS (bloom-in intro, zoom/pan, focus-constellation, color toggle, library filter, tooltips, click-through) | ATLAS-01 SC#1/#4 | Headless pytest cannot exercise the interactive Canvas renderer; the NiceGUI User harness only validates the SERVER-side component tree, NOT client JS/Canvas. The fetch/decode is proven by the automated Node golden test (04-T3), but visual/interaction FIDELITY is manual (04-T1/04-T2 draw/interaction fidelity — MEDIUM-2) | Open `/atlas` with the flag ON in a live web session; verify render, reduced-motion skip, EN/HE toggle + RTL chrome, zoom/pan/focus/color-toggle/library-filter, click-through to `/browse`, CLS-safe (reserved canvas) |
| Binary decode correctness across JS + Python | ATLAS-01 SC#1 | Automated via the golden fixture (NOT manual) | `python -m pytest tests/atlas_bake/test_atlas_golden_js.py -q` (JS decode == `golden-v1-expected.json` == Python decode in `tests/atlas_bake/test_atlas_bake.py`; sys_id via BigInt(str)) |
| Masking over the CLIENT-RENDERED DOM (tooltips/focus/search materialized by interactions) | ATLAS-01 SC#3 (HIGH-4) | Requires a real headless browser running the JS + a running server; scripted but human-run at go-live | `python scripts/capture_atlas_html.py --browser-dom https://genizahsearch.com --out-dir <dir>` (EN+HE, after readiness, exercising search/focus/tooltip) then `python scripts/check_atlas_masking.py --scan-asset <dir>` → exit 0 (plan 133-06 Task 4) |
| Homepage teaser card render + link | ATLAS-01 SC#6 | Live render smoke only for full fidelity | Open `/` with the flag ON + asset loaded; verify CLS-safe static card, EN/HE + RTL, links to `/atlas` (noindex) |
| Node-set completeness against the real research DB (EXACT eligible==placed, ~62,645; floor >= 62,414) | ATLAS-01 SC#1 / D-09 | Requires the 2.9 GB gitignored research DB (not in CI) | `python scripts/build_atlas_asset.py <research-db> --report` prints eligible_count==placed_count, missing=0, extra=0, placed >= 62,414 (plan 133-06 Task 1) |
| Live Brotli Content-Encoding negotiation | ATLAS-01 SC#4/#5 | Requires the deployed prod server (transport-level headers) | `curl -D- -H 'Accept-Encoding: br' https://genizahsearch.com/atlas-data/<asset_basename>.bin` -> `Content-Encoding: br`; without the header -> plain bytes, no `Content-Encoding`; `identity;q=0` -> 406 (plan 133-06 Task 4) |
| Masking scan over the REAL built asset + LOCAL (ASGI) and LIVE (browser) rendered output | ATLAS-01 SC#3 | Requires the real built asset + a rendered-page capture (local via ASGI, live via headless browser) | `python scripts/check_atlas_masking.py --scan-asset atlas_data/` + `--scan-asset <captured /atlas + / HTML/DOM>` → exit 0 (plan 133-06 Tasks 1 + 4) |
| Production go-live + rollback drill | ATLAS-01 SC#5 | Prod-touching; asset-first upload + flag flip + restart + rollback need human confirmation | Plan 133-06 Tasks 3-4: asset scp'd first (outside static root) → code deploy → flag set → restart → live render/Brotli/noindex/EN-HE/teaser/browser-DOM-masking smoke → flag-OFF rollback drill (clean-hide, nav/teaser gone, data route 404, rest of app intact) |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or a human-checkpoint (133-06 prod tasks 06-T3/06-T4 are checkpoint:human-verify by necessity — they touch production)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (the only human checkpoints are the two prod-deploy tasks in 133-06, each preceded by 06-T1/06-T2's automated gates)
- [x] Wave 0 covers all MISSING references (each plan co-creates its test scaffold; the golden fixture + decoder module + capture helper + four-surface test are enumerated)
- [x] No watch-mode flags
- [x] Feedback latency < 70s
- [x] `nyquist_compliant: true` set in frontmatter
- [x] MEDIUM-2 corrected: 04-T1/04-T2 are relabeled OFF the server render-smoke command — decode fidelity = Node golden (04-T3), draw/interaction fidelity = manual UAT, with `python -c` source scans as their automated verify
- [x] HIGH-3: 133-01 is ONE atomic task; the single terminal commit is gated on the green 3-pass scan + scrub (the phase's FIRST commit)
- [x] HIGH-4: the masking gate now scans the LIVE headless-browser CLIENT DOM (interactions exercised, EN+HE) — not just the server response
- [x] MEDIUM-6: a parametrized four-surface (page/data/nav/teaser) integration test covers OFF/asset-missing/ready behaviorally (06-T2)
- [x] MEDIUM-3 + stale-manifest: br/identity/* q-value negotiation with a reachable 406; manifest served no-cache + ETag + 304 with a stale-manifest transition test
- [x] NEW LOW: sys_id is BigUint64-only (no fallback, bake fails on invalid); deploy/release docs (DEPLOYMENT_TECHNICAL.md + CHANGELOG.md + README.md) added to 133-06

**Approval:** planner-approved 2026-07-20 (revised 2026-07-20 — Codex pre-flight rework: waves restructured to 5, 133-06 added, asset off `/static`, exact node-set, frozen schema + golden cross-language decode, DOM-XSS safety, Brotli q-negotiation, four-surface predicate, pinned atlas-bake CI; re-revised 2026-07-20 — Codex confirmation pass round 2: atomic 133-01, live browser-DOM masking, four-surface behavioral test, br/identity/* + no-cache manifest, sys_id no-fallback, deploy docs, 04-T1/04-T2 relabel, CI secret path)
