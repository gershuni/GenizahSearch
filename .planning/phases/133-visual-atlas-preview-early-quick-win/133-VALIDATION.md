---
phase: 133
slug: visual-atlas-preview-early-quick-win
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-07-20
---

# Phase 133 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini / conftest.py (GUI-tests split; run atlas/web tests with `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`) |
| **Quick run command** | `python -m pytest tests/test_atlas_*.py -q` |
| **Full suite command** | `python -m pytest tests/ -q` |
| **Estimated runtime** | ~60 seconds (targeted) / full suite deferred to CI |

---

## Sampling Rate

- **After every task commit:** Run `python -m pytest tests/test_atlas_*.py -q`
- **After every plan wave:** Run `python -m pytest tests/ -q` (targeted subset if full suite OOMs locally)
- **Before `/gsd:verify-work`:** Targeted atlas suite + masking-scan gate must be green
- **Max feedback latency:** 60 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | TBD | TBD | ATLAS-01 | T-133-XX / — | Masking scan finds zero M-source / sigla strings in built asset + rendered output | unit | `python scripts/atlas_masking_scan.py` (exact path TBD by planner) | ❌ W0 | ⬜ pending |

*The planner fills this map from the final PLAN.md task IDs. Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_atlas_masking_scan.py` — asserts the reusable masking scan rejects known M-source/sigla patterns and passes a clean asset (D-07 → forerunner of DATA-05)
- [ ] `tests/test_atlas_page.py` — `/atlas` route hides cleanly when the atlas-preview flag is OFF or the asset is absent (D-13), zero errors, rest of app untouched
- [ ] `tests/test_atlas_bake.py` — offline bake emits a masking-clean static asset within the PERF-01 byte cap with deterministic seed + version metadata

*Planner confirms/renames these against the actual plan task IDs.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Canvas 2D render fidelity (bloom-in intro, zoom/pan, focus-constellation, color toggle) | ATLAS-01 SC#1/#4 | Headless pytest cannot exercise the interactive Canvas renderer; NiceGUI render path needs a live client | Open `/atlas` with the flag ON in a live web session; verify render, reduced-motion skip, EN/HE toggle + RTL chrome, click-through to `/browse`, CLS-safe (reserved canvas) |
| Homepage teaser card render + link | ATLAS-01 (teaser exception) | Live render smoke only | Open `/` with the flag ON; verify CLS-safe static card, `noindex`, EN/HE + RTL, links to `/atlas` |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
